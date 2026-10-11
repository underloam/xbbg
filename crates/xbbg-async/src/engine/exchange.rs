use arrow_array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
};
use chrono::NaiveDate;

use xbbg_ext::markets::overrides;
use xbbg_ext::{
    ExchangeInfo, ExchangeInfoSource, MarketInfo, MarketTiming, derive_sessions,
    infer_timezone_from_country, market_timing,
};

use crate::errors::BlpAsyncError;
use crate::services::{ExtractorType, Operation, Service};

use super::{Engine, RequestParams};

const EXCHANGE_FIELDS: [&str; 7] = [
    "IANA_TIME_ZONE",
    "TIME_ZONE_NUM",
    "ID_MIC_PRIM_EXCH",
    "EXCH_CODE",
    "COUNTRY_ISO",
    "TRADING_DAY_START_TIME_EOD",
    "TRADING_DAY_END_TIME_EOD",
];

const FUTURES_HOURS_FIELDS: [&str; 1] = ["FUT_TRADING_HRS"];

const MARKET_FIELDS: [&str; 3] = ["EXCH_CODE", "ID_MIC_PRIM_EXCH", "IANA_TIME_ZONE"];

const FUT_GEN_MONTH_FIELDS: [&str; 1] = ["FUT_GEN_MONTH"];

impl Engine {
    /// Query Bloomberg exchange metadata and derive sessions.
    pub async fn fetch_exchange_info(&self, ticker: &str) -> Result<ExchangeInfo, BlpAsyncError> {
        let trimmed = ticker.trim();
        if trimmed.is_empty() {
            return Err(BlpAsyncError::ConfigError {
                detail: "ticker is required".to_string(),
            });
        }

        let batch = self
            .fetch_exchange_fields(trimmed, &EXCHANGE_FIELDS)
            .await?;
        let mut info = parse_exchange_info(trimmed, &batch);

        if info.sessions.day.is_none() {
            match self
                .fetch_exchange_fields(trimmed, &FUTURES_HOURS_FIELDS)
                .await
            {
                Ok(futures_batch) => apply_futures_hours(&mut info, &futures_batch),
                Err(e) => xbbg_log::warn!(
                    ticker = trimmed,
                    error = %e,
                    "futures-hours exchange fallback failed"
                ),
            }
        }

        Ok(info)
    }

    async fn fetch_exchange_fields(
        &self,
        ticker: &str,
        fields: &[&str],
    ) -> Result<arrow_array::RecordBatch, BlpAsyncError> {
        let params = RequestParams {
            service: Service::RefData.to_string(),
            operation: Operation::ReferenceData.to_string(),
            extractor: ExtractorType::RefData,
            extractor_set: true,
            securities: Some(vec![ticker.to_string()]),
            fields: Some(fields.iter().map(|s| s.to_string()).collect()),
            format: Some("wide".to_string()),
            ..Default::default()
        };

        self.request_without_intraday_transform(params).await
    }

    /// Query lightweight market metadata used by higher-level APIs.
    pub async fn fetch_market_info(&self, ticker: &str) -> Result<MarketInfo, BlpAsyncError> {
        let trimmed = ticker.trim();
        if trimmed.is_empty() {
            return Err(BlpAsyncError::ConfigError {
                detail: "ticker is required".to_string(),
            });
        }

        let batch = self.fetch_exchange_fields(trimmed, &MARKET_FIELDS).await?;

        let exch =
            get_string(&batch, "EXCH_CODE").or_else(|| get_string(&batch, "ID_MIC_PRIM_EXCH"));
        let tz = get_string(&batch, "IANA_TIME_ZONE");
        let freq = if should_query_fut_gen_month(trimmed) {
            match self
                .fetch_exchange_fields(trimmed, &FUT_GEN_MONTH_FIELDS)
                .await
            {
                Ok(batch) => get_string(&batch, "FUT_GEN_MONTH"),
                Err(e) => {
                    xbbg_log::warn!(
                        ticker = trimmed,
                        error = %e,
                        "futures cycle metadata lookup failed"
                    );
                    None
                }
            }
        } else {
            None
        };
        let is_fut = freq.as_ref().is_some_and(|s| !s.trim().is_empty())
            || should_query_fut_gen_month(trimmed);

        Ok(MarketInfo {
            exch,
            tz,
            freq,
            is_fut,
        })
    }

    /// Full exchange-resolution waterfall:
    /// override -> cache -> bloomberg -> inferred/fallback.
    pub async fn resolve_exchange(&self, ticker: &str) -> ExchangeInfo {
        let trimmed = ticker.trim();
        if trimmed.is_empty() {
            return ExchangeInfo::fallback("");
        }

        match overrides::get_exchange_override(trimmed) {
            Ok(Some(info)) => return info,
            Ok(None) => {}
            Err(e) => {
                xbbg_log::warn!(ticker = trimmed, error = %e, "resolve_exchange override lookup failed")
            }
        }

        if let Some(info) = self.exchange_cache.get(trimmed) {
            return info;
        }

        match self.fetch_exchange_info(trimmed).await {
            Ok(info) => {
                if info.source != ExchangeInfoSource::Fallback {
                    self.exchange_cache.put(trimmed, info.clone());
                }
                info
            }
            Err(e) => {
                xbbg_log::warn!(ticker = trimmed, error = %e, "resolve_exchange failed; using fallback");
                ExchangeInfo::fallback(trimmed)
            }
        }
    }

    pub fn invalidate_exchange_cache(&self, ticker: Option<&str>) -> Result<(), String> {
        self.exchange_cache.invalidate(ticker)
    }

    pub fn save_exchange_cache(&self) -> Result<(), String> {
        self.exchange_cache.save_to_disk()
    }

    /// Resolve market timing by first resolving exchange metadata.
    pub async fn resolve_market_timing(
        &self,
        ticker: &str,
        date: NaiveDate,
        timing: MarketTiming,
        target_tz: Option<&str>,
    ) -> Result<String, BlpAsyncError> {
        let info = self.resolve_exchange(ticker).await;
        market_timing(&info, date, timing, target_tz).map_err(|e| BlpAsyncError::ConfigError {
            detail: e.to_string(),
        })
    }
}

fn parse_exchange_info(ticker: &str, batch: &arrow_array::RecordBatch) -> ExchangeInfo {
    if batch.num_rows() == 0 {
        return ExchangeInfo::fallback(ticker.to_string());
    }

    let mic = get_string(batch, "ID_MIC_PRIM_EXCH");
    let exch_code = get_string(batch, "EXCH_CODE");
    let country = get_string(batch, "COUNTRY_ISO");
    let iana_tz = get_string(batch, "IANA_TIME_ZONE");
    let utc_offset = get_f64(batch, "TIME_ZONE_NUM");

    let (timezone, source) = if let Some(tz) = iana_tz {
        (tz, ExchangeInfoSource::Bloomberg)
    } else if let Some(c) = country.as_deref().and_then(infer_timezone_from_country) {
        (c.to_string(), ExchangeInfoSource::Inferred)
    } else {
        ("UTC".to_string(), ExchangeInfoSource::Fallback)
    };

    let start = get_string(batch, "TRADING_DAY_START_TIME_EOD");
    let end = get_string(batch, "TRADING_DAY_END_TIME_EOD");
    let fut = get_string(batch, "FUT_TRADING_HRS").and_then(|v| parse_futures_hours(&v));

    let (day_start, day_end) = match (start, end, fut) {
        (Some(s), Some(e), _) => (s, e),
        (None, None, Some((s, e))) => (s, e),
        _ => (String::new(), String::new()),
    };

    let sessions = if !day_start.is_empty() && !day_end.is_empty() {
        derive_sessions(&day_start, &day_end, mic.as_deref(), exch_code.as_deref())
    } else {
        xbbg_ext::SessionWindows::default()
    };

    ExchangeInfo {
        ticker: ticker.to_string(),
        mic,
        exch_code,
        timezone,
        utc_offset,
        sessions,
        source,
        cached_at: None,
    }
}

fn apply_futures_hours(info: &mut ExchangeInfo, batch: &arrow_array::RecordBatch) {
    let Some((day_start, day_end)) =
        get_string(batch, "FUT_TRADING_HRS").and_then(|v| parse_futures_hours(&v))
    else {
        return;
    };

    info.sessions = derive_sessions(
        &day_start,
        &day_end,
        info.mic.as_deref(),
        info.exch_code.as_deref(),
    );
}

fn should_query_fut_gen_month(ticker: &str) -> bool {
    let mut parts = ticker.split_whitespace();
    let Some(root) = parts.next() else {
        return false;
    };
    let asset_class = ticker.split_whitespace().last().unwrap_or_default();

    match asset_class {
        "Comdty" => true,
        "Index" | "Curncy" => root.chars().any(|c| c.is_ascii_digit()),
        _ => false,
    }
}

fn parse_futures_hours(raw: &str) -> Option<(String, String)> {
    let trimmed = raw.trim();
    let (start, end) = trimmed.split_once('-')?;
    let s = normalize_hhmm(start.trim())?;
    let e = normalize_hhmm(end.trim())?;
    Some((s, e))
}

fn normalize_hhmm(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if let Some((hh, mm)) = trimmed.split_once(':') {
        let h: u32 = hh.parse().ok()?;
        let m: u32 = mm.parse().ok()?;
        if h <= 23 && m <= 59 {
            return Some(format!("{h:02}:{m:02}"));
        }
        return None;
    }
    if trimmed.len() == 4 && trimmed.chars().all(|c| c.is_ascii_digit()) {
        let h: u32 = trimmed[0..2].parse().ok()?;
        let m: u32 = trimmed[2..4].parse().ok()?;
        if h <= 23 && m <= 59 {
            return Some(format!("{h:02}:{m:02}"));
        }
    }
    None
}

fn get_string(batch: &arrow_array::RecordBatch, col: &str) -> Option<String> {
    if batch.num_rows() == 0 {
        return None;
    }
    if let Some(arr) = batch.column_by_name(col) {
        return value_as_string(arr.as_ref(), 0).and_then(clean_value);
    }

    get_long_field_value(batch, col).and_then(clean_value)
}

fn get_f64(batch: &arrow_array::RecordBatch, col: &str) -> Option<f64> {
    if batch.num_rows() == 0 {
        return None;
    }
    if let Some(arr) = batch.column_by_name(col) {
        if let Some(v) = arr.as_any().downcast_ref::<Float64Array>() {
            return (!v.is_null(0)).then(|| v.value(0));
        }
        if let Some(v) = arr.as_any().downcast_ref::<Int32Array>() {
            return (!v.is_null(0)).then(|| v.value(0) as f64);
        }
        if let Some(v) = arr.as_any().downcast_ref::<Int64Array>() {
            return (!v.is_null(0)).then(|| v.value(0) as f64);
        }
        if let Some(v) = arr.as_any().downcast_ref::<StringArray>() {
            if v.is_null(0) {
                return None;
            }
            return parse_f64(v.value(0));
        }
    }

    get_long_field_value(batch, col).and_then(|s| parse_f64(&s))
}

fn get_long_field_value(batch: &arrow_array::RecordBatch, field_name: &str) -> Option<String> {
    let fields = batch
        .column_by_name("field")?
        .as_any()
        .downcast_ref::<StringArray>()?;
    let values = batch.column_by_name("value")?;

    for row in 0..batch.num_rows() {
        if fields.is_null(row) {
            continue;
        }
        if !fields.value(row).eq_ignore_ascii_case(field_name) {
            continue;
        }
        return value_as_string(values.as_ref(), row);
    }
    None
}

fn value_as_string(arr: &dyn Array, row: usize) -> Option<String> {
    if let Some(v) = arr.as_any().downcast_ref::<StringArray>() {
        return (!v.is_null(row)).then(|| v.value(row).to_string());
    }
    if let Some(v) = arr.as_any().downcast_ref::<Int32Array>() {
        return (!v.is_null(row)).then(|| v.value(row).to_string());
    }
    if let Some(v) = arr.as_any().downcast_ref::<Int64Array>() {
        return (!v.is_null(row)).then(|| v.value(row).to_string());
    }
    if let Some(v) = arr.as_any().downcast_ref::<Float64Array>() {
        return (!v.is_null(row)).then(|| v.value(row).to_string());
    }
    if let Some(v) = arr.as_any().downcast_ref::<Time32SecondArray>() {
        return (!v.is_null(row)).then(|| format_seconds(v.value(row) as i64));
    }
    if let Some(v) = arr.as_any().downcast_ref::<Time32MillisecondArray>() {
        return (!v.is_null(row)).then(|| format_seconds((v.value(row) as i64) / 1_000));
    }
    if let Some(v) = arr.as_any().downcast_ref::<Time64MicrosecondArray>() {
        return (!v.is_null(row)).then(|| format_seconds(v.value(row) / 1_000_000));
    }
    if let Some(v) = arr.as_any().downcast_ref::<Time64NanosecondArray>() {
        return (!v.is_null(row)).then(|| format_seconds(v.value(row) / 1_000_000_000));
    }
    if let Some(v) = arr.as_any().downcast_ref::<BooleanArray>() {
        return (!v.is_null(row)).then(|| v.value(row).to_string());
    }
    None
}

fn format_seconds(seconds: i64) -> String {
    let seconds = seconds.rem_euclid(24 * 60 * 60);
    format!(
        "{:02}:{:02}:{:02}",
        seconds / 3600,
        (seconds % 3600) / 60,
        seconds % 60
    )
}

fn clean_value(raw: String) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("nan") {
        return None;
    }
    Some(trimmed.to_string())
}

fn parse_f64(raw: &str) -> Option<f64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("nan") {
        return None;
    }
    trimmed.parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use arrow_array::{
        ArrayRef, Float64Array, StringArray, Time32MillisecondArray, Time32SecondArray,
        Time64MicrosecondArray, Time64NanosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema};

    use super::{apply_futures_hours, parse_exchange_info, should_query_fut_gen_month};
    use xbbg_ext::ExchangeInfoSource;

    fn single_row_batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect();
        let arrays: Vec<ArrayRef> = columns.into_iter().map(|(_, array)| array).collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("valid single-row batch")
    }

    fn long_batch(rows: Vec<(&str, Option<&str>)>) -> RecordBatch {
        let fields = Arc::new(StringArray::from(
            rows.iter().map(|(f, _)| Some(*f)).collect::<Vec<_>>(),
        )) as ArrayRef;
        let values = Arc::new(StringArray::from(
            rows.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
        )) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            Field::new("field", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(schema, vec![fields, values]).expect("valid long batch")
    }

    #[test]
    fn test_parse_exchange_info_infers_timezone_when_iana_missing() {
        let batch = single_row_batch(vec![
            (
                "IANA_TIME_ZONE",
                Arc::new(StringArray::from(vec![None::<&str>])),
            ),
            (
                "TIME_ZONE_NUM",
                Arc::new(Float64Array::from(vec![Some(-5.0)])),
            ),
            (
                "ID_MIC_PRIM_EXCH",
                Arc::new(StringArray::from(vec![Some("XNGS")])) as ArrayRef,
            ),
            (
                "EXCH_CODE",
                Arc::new(StringArray::from(vec![Some("US")])) as ArrayRef,
            ),
            (
                "COUNTRY_ISO",
                Arc::new(StringArray::from(vec![Some("US")])) as ArrayRef,
            ),
            (
                "TRADING_DAY_START_TIME_EOD",
                Arc::new(StringArray::from(vec![Some("0930")])) as ArrayRef,
            ),
            (
                "TRADING_DAY_END_TIME_EOD",
                Arc::new(StringArray::from(vec![Some("1600")])) as ArrayRef,
            ),
            (
                "FUT_TRADING_HRS",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
        ]);

        let info = parse_exchange_info("AAPL US Equity", &batch);
        assert_eq!(info.source, ExchangeInfoSource::Inferred);
        assert_eq!(info.timezone, "America/New_York");
        assert_eq!(info.utc_offset, Some(-5.0));
        assert_eq!(
            info.sessions.day,
            Some(("09:30".to_string(), "16:00".to_string()))
        );
        assert_eq!(
            info.sessions.allday,
            Some(("04:00".to_string(), "20:00".to_string()))
        );
    }

    #[test]
    fn test_parse_exchange_info_uses_futures_hours_when_regular_hours_missing() {
        let batch = single_row_batch(vec![
            (
                "IANA_TIME_ZONE",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            (
                "TIME_ZONE_NUM",
                Arc::new(Float64Array::from(vec![None::<f64>])) as ArrayRef,
            ),
            (
                "ID_MIC_PRIM_EXCH",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            (
                "EXCH_CODE",
                Arc::new(StringArray::from(vec![Some("CME")])) as ArrayRef,
            ),
            (
                "COUNTRY_ISO",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            (
                "TRADING_DAY_START_TIME_EOD",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            (
                "TRADING_DAY_END_TIME_EOD",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            (
                "FUT_TRADING_HRS",
                Arc::new(StringArray::from(vec![Some("18:00-17:00")])) as ArrayRef,
            ),
        ]);

        let info = parse_exchange_info("ES1 Index", &batch);
        assert_eq!(info.source, ExchangeInfoSource::Fallback);
        assert_eq!(info.timezone, "UTC");
        assert_eq!(
            info.sessions.day,
            Some(("18:00".to_string(), "17:00".to_string()))
        );
        assert_eq!(info.sessions.allday, info.sessions.day);
        assert_eq!(info.sessions.pre, None);
        assert_eq!(info.sessions.post, None);
    }

    #[test]
    fn test_apply_futures_hours_populates_missing_sessions() {
        let mut info = parse_exchange_info(
            "TY1 Comdty",
            &single_row_batch(vec![
                (
                    "IANA_TIME_ZONE",
                    Arc::new(StringArray::from(vec![Some("America/Chicago")])) as ArrayRef,
                ),
                (
                    "TIME_ZONE_NUM",
                    Arc::new(Float64Array::from(vec![Some(-6.0)])) as ArrayRef,
                ),
                (
                    "ID_MIC_PRIM_EXCH",
                    Arc::new(StringArray::from(vec![Some("XCBT")])) as ArrayRef,
                ),
                (
                    "EXCH_CODE",
                    Arc::new(StringArray::from(vec![Some("CBT")])) as ArrayRef,
                ),
                (
                    "COUNTRY_ISO",
                    Arc::new(StringArray::from(vec![Some("US")])) as ArrayRef,
                ),
                (
                    "TRADING_DAY_START_TIME_EOD",
                    Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
                ),
                (
                    "TRADING_DAY_END_TIME_EOD",
                    Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
                ),
            ]),
        );
        assert_eq!(info.sessions.day, None);

        let futures_batch = single_row_batch(vec![(
            "FUT_TRADING_HRS",
            Arc::new(StringArray::from(vec![Some("18:00-17:00")])) as ArrayRef,
        )]);
        apply_futures_hours(&mut info, &futures_batch);

        assert_eq!(
            info.sessions.day,
            Some(("18:00".to_string(), "17:00".to_string()))
        );
        assert_eq!(info.sessions.allday, info.sessions.day);
    }

    #[test]
    fn test_parse_exchange_info_falls_back_to_utc_without_iana_or_country() {
        let batch = single_row_batch(vec![
            (
                "IANA_TIME_ZONE",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            (
                "TIME_ZONE_NUM",
                Arc::new(Float64Array::from(vec![None::<f64>])) as ArrayRef,
            ),
            (
                "ID_MIC_PRIM_EXCH",
                Arc::new(StringArray::from(vec![Some("XLON")])) as ArrayRef,
            ),
            (
                "EXCH_CODE",
                Arc::new(StringArray::from(vec![Some("LN")])) as ArrayRef,
            ),
            (
                "COUNTRY_ISO",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            (
                "TRADING_DAY_START_TIME_EOD",
                Arc::new(StringArray::from(vec![Some("08:00")])) as ArrayRef,
            ),
            (
                "TRADING_DAY_END_TIME_EOD",
                Arc::new(StringArray::from(vec![Some("16:30")])) as ArrayRef,
            ),
            (
                "FUT_TRADING_HRS",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
        ]);

        let info = parse_exchange_info("VOD LN Equity", &batch);
        assert_eq!(info.source, ExchangeInfoSource::Fallback);
        assert_eq!(info.timezone, "UTC");
        assert_eq!(
            info.sessions.day,
            Some(("08:00".to_string(), "16:30".to_string()))
        );
    }

    #[test]
    fn test_parse_exchange_info_handles_long_refdata_shape() {
        let batch = long_batch(vec![
            ("IANA_TIME_ZONE", Some("America/New_York")),
            ("TIME_ZONE_NUM", Some("-5")),
            ("ID_MIC_PRIM_EXCH", Some("XNGS")),
            ("EXCH_CODE", Some("US")),
            ("COUNTRY_ISO", Some("US")),
            ("TRADING_DAY_START_TIME_EOD", Some("09:30:00.000000")),
            ("TRADING_DAY_END_TIME_EOD", Some("16:30:00.000000")),
            ("FUT_TRADING_HRS", None),
        ]);

        let info = parse_exchange_info("AAPL US Equity", &batch);
        assert_eq!(info.source, ExchangeInfoSource::Bloomberg);
        assert_eq!(info.timezone, "America/New_York");
        assert_eq!(info.mic.as_deref(), Some("XNGS"));
        assert_eq!(info.exch_code.as_deref(), Some("US"));
        assert_eq!(info.utc_offset, Some(-5.0));
        assert_eq!(
            info.sessions.day,
            Some(("09:30".to_string(), "16:30".to_string()))
        );
    }

    #[test]
    fn test_parse_exchange_info_handles_time_valued_session_fields() {
        let batch = single_row_batch(vec![
            (
                "IANA_TIME_ZONE",
                Arc::new(StringArray::from(vec![Some("America/New_York")])) as ArrayRef,
            ),
            (
                "TIME_ZONE_NUM",
                Arc::new(Float64Array::from(vec![Some(-5.0)])) as ArrayRef,
            ),
            (
                "ID_MIC_PRIM_EXCH",
                Arc::new(StringArray::from(vec![Some("XNGS")])) as ArrayRef,
            ),
            (
                "EXCH_CODE",
                Arc::new(StringArray::from(vec![Some("US")])) as ArrayRef,
            ),
            (
                "COUNTRY_ISO",
                Arc::new(StringArray::from(vec![Some("US")])) as ArrayRef,
            ),
            (
                "TRADING_DAY_START_TIME_EOD",
                Arc::new(Time64MicrosecondArray::from(vec![Some(
                    (9_i64 * 60 * 60 + 30 * 60) * 1_000_000,
                )])) as ArrayRef,
            ),
            (
                "TRADING_DAY_END_TIME_EOD",
                Arc::new(Time64MicrosecondArray::from(vec![Some(
                    (16_i64 * 60 * 60 + 30 * 60) * 1_000_000,
                )])) as ArrayRef,
            ),
            (
                "FUT_TRADING_HRS",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
        ]);

        let info = parse_exchange_info("AAPL US Equity", &batch);
        assert_eq!(
            info.sessions.day,
            Some(("09:30".to_string(), "16:30".to_string()))
        );
    }

    #[test]
    fn test_parse_exchange_info_uses_japan_market_rule_close() {
        let batch = single_row_batch(vec![
            (
                "IANA_TIME_ZONE",
                Arc::new(StringArray::from(vec![Some("Asia/Tokyo")])) as ArrayRef,
            ),
            (
                "TIME_ZONE_NUM",
                Arc::new(Float64Array::from(vec![Some(9.0)])) as ArrayRef,
            ),
            (
                "ID_MIC_PRIM_EXCH",
                Arc::new(StringArray::from(vec![Some("XTKS")])) as ArrayRef,
            ),
            (
                "EXCH_CODE",
                Arc::new(StringArray::from(vec![Some("JP")])) as ArrayRef,
            ),
            (
                "COUNTRY_ISO",
                Arc::new(StringArray::from(vec![Some("JP")])) as ArrayRef,
            ),
            (
                "TRADING_DAY_START_TIME_EOD",
                Arc::new(Time64MicrosecondArray::from(vec![Some(
                    (20_i64 * 60 * 60) * 1_000_000,
                )])) as ArrayRef,
            ),
            (
                "TRADING_DAY_END_TIME_EOD",
                Arc::new(Time64MicrosecondArray::from(vec![Some(
                    (2_i64 * 60 * 60 + 45 * 60) * 1_000_000,
                )])) as ArrayRef,
            ),
            (
                "FUT_TRADING_HRS",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
        ]);

        let info = parse_exchange_info("7203 JP Equity", &batch);
        assert_eq!(info.timezone, "Asia/Tokyo");
        assert_eq!(
            info.sessions.day,
            Some(("09:00".to_string(), "15:30".to_string()))
        );
        assert_eq!(
            info.sessions.pm,
            Some(("12:30".to_string(), "15:30".to_string()))
        );
    }

    #[test]
    fn test_parse_exchange_info_supports_arrow_time_units() {
        let second =
            Arc::new(Time32SecondArray::from(vec![Some(9 * 60 * 60 + 30 * 60)])) as ArrayRef;
        let millisecond = Arc::new(Time32MillisecondArray::from(vec![Some(
            (16 * 60 * 60 + 30 * 60) * 1_000,
        )])) as ArrayRef;
        let microsecond = Arc::new(Time64MicrosecondArray::from(vec![Some(
            (9_i64 * 60 * 60 + 30 * 60) * 1_000_000,
        )])) as ArrayRef;
        let nanosecond = Arc::new(Time64NanosecondArray::from(vec![Some(
            (16_i64 * 60 * 60 + 30 * 60) * 1_000_000_000,
        )])) as ArrayRef;

        for (start, end) in [(second, millisecond), (microsecond, nanosecond)] {
            let batch = single_row_batch(vec![
                (
                    "IANA_TIME_ZONE",
                    Arc::new(StringArray::from(vec![Some("America/New_York")])) as ArrayRef,
                ),
                (
                    "TIME_ZONE_NUM",
                    Arc::new(Float64Array::from(vec![Some(-5.0)])) as ArrayRef,
                ),
                (
                    "ID_MIC_PRIM_EXCH",
                    Arc::new(StringArray::from(vec![Some("XNGS")])) as ArrayRef,
                ),
                (
                    "EXCH_CODE",
                    Arc::new(StringArray::from(vec![Some("US")])) as ArrayRef,
                ),
                (
                    "COUNTRY_ISO",
                    Arc::new(StringArray::from(vec![Some("US")])) as ArrayRef,
                ),
                ("TRADING_DAY_START_TIME_EOD", start),
                ("TRADING_DAY_END_TIME_EOD", end),
                (
                    "FUT_TRADING_HRS",
                    Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
                ),
            ]);

            let info = parse_exchange_info("AAPL US Equity", &batch);
            assert_eq!(
                info.sessions.day,
                Some(("09:30".to_string(), "16:30".to_string()))
            );
        }
    }

    #[test]
    fn test_should_query_fut_gen_month_only_for_futures_like_tickers() {
        assert!(should_query_fut_gen_month("TY1 Comdty"));
        assert!(should_query_fut_gen_month("ES1 Index"));
        assert!(!should_query_fut_gen_month("AAPL US Equity"));
        assert!(!should_query_fut_gen_month("7203 JP Equity"));
        assert!(!should_query_fut_gen_month("SPX Index"));
    }

    #[test]
    fn test_single_row_batch_schema_uses_input_types() {
        let batch = single_row_batch(vec![
            (
                "S",
                Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef,
            ),
            (
                "F",
                Arc::new(Float64Array::from(vec![Some(1.0)])) as ArrayRef,
            ),
        ]);

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Float64);
    }
}
