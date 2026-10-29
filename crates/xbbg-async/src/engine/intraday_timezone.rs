//! Intraday bar/tick timezone handling (request wall-clock → UTC, output UTC → display TZ).
//!
//! Bloomberg `startDateTime` / `endDateTime` are sent as UTC instants; tick/bar `time` is
//! returned as Arrow `timestamp[us, tz=UTC]`. Callers pass `request_tz` / `output_tz` on
//! [`crate::engine::RequestParams`] to interpret naive inputs and relabel output timestamps.

use std::str::FromStr;

use arrow_array::RecordBatch;
use arrow_array::{Array, TimestampMicrosecondArray};
use arrow_schema::ArrowError;
use arrow_schema::Field;
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;

use crate::errors::BlpAsyncError;
use crate::services::Operation;

use super::Engine;
use super::RequestParams;

/// Resolve `UTC` | `local` | `exchange` | `NY`/`LN`/… | IANA into an IANA timezone name.
pub(crate) async fn resolve_tz_label(
    engine: &Engine,
    label: &str,
    security: Option<&str>,
) -> Result<String, BlpAsyncError> {
    let s = label.trim();
    if s.is_empty() || s.eq_ignore_ascii_case("utc") {
        return Ok("UTC".to_string());
    }
    if s.eq_ignore_ascii_case("local") {
        return Ok(local_iana_timezone());
    }
    if s.eq_ignore_ascii_case("exchange") {
        let sec = security.ok_or_else(|| BlpAsyncError::ConfigError {
            detail: "output_tz/request_tz='exchange' requires a security on the request"
                .to_string(),
        })?;
        let info = engine.resolve_exchange(sec).await;
        if info.timezone.is_empty() {
            return Err(BlpAsyncError::ConfigError {
                detail: format!("could not resolve exchange timezone for {sec}"),
            });
        }
        return Ok(info.timezone);
    }
    if let Some(iana) = alias_timezone(s) {
        return Ok(iana.to_string());
    }
    if s.contains(' ') {
        let info = engine.resolve_exchange(s).await;
        if !info.timezone.is_empty() {
            return Ok(info.timezone);
        }
    }
    // Validate IANA
    let _: Tz = s.parse().map_err(|_| BlpAsyncError::ConfigError {
        detail: format!("unknown timezone label: {label}"),
    })?;
    Ok(s.to_string())
}

fn alias_timezone(label: &str) -> Option<&'static str> {
    match label.to_uppercase().as_str() {
        "NY" => Some("America/New_York"),
        "LN" => Some("Europe/London"),
        "TK" => Some("Asia/Tokyo"),
        "HK" => Some("Asia/Hong_Kong"),
        _ => None,
    }
}

fn local_iana_timezone() -> String {
    iana_time_zone::get_timezone().unwrap_or_else(|_| "UTC".to_string())
}

fn intraday_operation(params: &RequestParams) -> bool {
    matches!(
        Operation::from_str(params.effective_operation()),
        Ok(Operation::IntradayBar | Operation::IntradayTick)
    )
}

/// If `request_tz` is set, interpret naive `start_datetime` / `end_datetime` in that zone and
/// return UTC ISO strings for Bloomberg.
pub(crate) async fn resolve_intraday_request_datetimes(
    engine: &Engine,
    params: &RequestParams,
) -> Result<Option<(String, String)>, BlpAsyncError> {
    if !intraday_operation(params) {
        return Ok(None);
    }
    let Some(start) = params.start_datetime.as_ref() else {
        return Ok(None);
    };
    let Some(end) = params.end_datetime.as_ref() else {
        return Ok(None);
    };

    let label = match params.request_tz.as_deref() {
        None | Some("") => return Ok(None),
        Some(s) if s.eq_ignore_ascii_case("utc") => return Ok(None),
        Some(s) => s,
    };

    let iana = resolve_tz_label(engine, label, params.security.as_deref()).await?;

    let start_utc = wall_to_utc_iso(start, &iana)?;
    let end_utc = wall_to_utc_iso(end, &iana)?;
    Ok(Some((start_utc, end_utc)))
}

fn wall_to_utc_iso(input: &str, wall_tz_iana: &str) -> Result<String, BlpAsyncError> {
    match parse_user_datetime(input)? {
        UserDateTime::Utc(dt) => Ok(format_utc_iso(&dt.with_timezone(&Utc))),
        UserDateTime::Naive(naive) => {
            let tz: Tz = wall_tz_iana
                .parse()
                .map_err(|_| BlpAsyncError::ConfigError {
                    detail: format!("invalid IANA timezone: {wall_tz_iana}"),
                })?;
            let local = tz.from_local_datetime(&naive).single().ok_or_else(|| {
                BlpAsyncError::ConfigError {
                    detail: format!("ambiguous or invalid local datetime: {input}"),
                }
            })?;
            let utc = local.with_timezone(&Utc);
            Ok(format_utc_iso(&utc))
        }
    }
}

enum UserDateTime {
    Utc(DateTime<Utc>),
    Naive(NaiveDateTime),
}

fn parse_user_datetime(input: &str) -> Result<UserDateTime, BlpAsyncError> {
    let t = input.trim().replace(' ', "T");

    if let Ok(dt) = DateTime::parse_from_rfc3339(&t) {
        return Ok(UserDateTime::Utc(dt.with_timezone(&Utc)));
    }

    let t_z = if t.ends_with('Z') && !t.contains('+') {
        format!("{}+00:00", t.trim_end_matches('Z'))
    } else {
        t.clone()
    };
    if let Ok(dt) = DateTime::parse_from_rfc3339(&t_z) {
        return Ok(UserDateTime::Utc(dt.with_timezone(&Utc)));
    }

    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ] {
        if let Ok(n) = NaiveDateTime::parse_from_str(&t, fmt) {
            return Ok(UserDateTime::Naive(n));
        }
    }

    if let Ok(d) = NaiveDate::parse_from_str(&t, "%Y-%m-%d") {
        let Some(n) = d.and_hms_opt(0, 0, 0) else {
            return Err(BlpAsyncError::ConfigError {
                detail: format!("invalid date: {input}"),
            });
        };
        return Ok(UserDateTime::Naive(n));
    }

    Err(BlpAsyncError::ConfigError {
        detail: format!("could not parse datetime: {input}"),
    })
}

/// Parse a prepared request's start/end datetime as a UTC instant. Naive values
/// are UTC by then: Bloomberg reads them as UTC, and `request_tz` conversion
/// already rewrote wall-clock inputs to UTC.
pub(crate) fn parse_utc_instant(input: &str) -> Option<DateTime<Utc>> {
    match parse_user_datetime(input).ok()? {
        UserDateTime::Utc(instant) => Some(instant),
        UserDateTime::Naive(naive) => Some(naive.and_utc()),
    }
}

fn format_utc_iso(dt: &DateTime<Utc>) -> String {
    let base = dt.format("%Y-%m-%dT%H:%M:%S").to_string();
    let micros = dt.timestamp_subsec_micros();
    if micros == 0 {
        base
    } else {
        format!("{base}.{:06}", micros)
    }
}

/// Relabel `time` column from UTC to `output_tz` (same instants; Arrow tz metadata only).
pub(crate) async fn apply_intraday_output_timezone(
    engine: &Engine,
    batch: RecordBatch,
    params: &RequestParams,
) -> Result<RecordBatch, BlpAsyncError> {
    if !intraday_operation(params) {
        return Ok(batch);
    }
    let label = match params.output_tz.as_deref() {
        None | Some("") => return Ok(batch),
        Some(s) if s.eq_ignore_ascii_case("utc") => return Ok(batch),
        Some(s) => s,
    };

    let iana = resolve_tz_label(engine, label, params.security.as_deref()).await?;
    apply_output_timezone_batch(batch, &iana, None).map_err(|e| BlpAsyncError::ConfigError {
        detail: format!("intraday output timezone: {e}"),
    })
}

/// Relabel the timestamp wrapper, reusing a final schema cached for the same
/// source schema and timezone when available.
pub(crate) fn apply_output_timezone_batch(
    batch: RecordBatch,
    iana: &str,
    output_schema: Option<SchemaRef>,
) -> Result<RecordBatch, ArrowError> {
    if iana == "UTC" {
        return match output_schema {
            Some(schema) => batch.with_schema(schema),
            None => Ok(batch),
        };
    }

    let schema = batch.schema();
    let ti = schema
        .fields()
        .iter()
        .position(|f| f.name() == "time")
        .filter(|&ti| {
            matches!(
                batch.column(ti).data_type(),
                DataType::Timestamp(TimeUnit::Microsecond, Some(_))
            )
        });
    let Some(ti) = ti else {
        return match output_schema {
            Some(schema) => batch.with_schema(schema),
            None => Ok(batch),
        };
    };
    let col = batch.column(ti);

    let ts = col
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| {
            ArrowError::ComputeError("time column: expected TimestampMicrosecondArray".to_string())
        })?;

    let timezone = output_schema
        .as_ref()
        .and_then(|schema| match schema.field(ti).data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)) => Some(timezone.clone()),
            _ => None,
        })
        .unwrap_or_else(|| iana.into());
    let new_ts = ts.clone().with_timezone(timezone);
    let new_schema = output_schema.unwrap_or_else(|| {
        let new_field = Field::new("time", new_ts.data_type().clone(), true);
        let mut fields: Vec<_> = schema.fields().to_vec();
        fields[ti] = std::sync::Arc::new(new_field);
        std::sync::Arc::new(arrow_schema::Schema::new_with_metadata(
            fields,
            schema.metadata().clone(),
        ))
    });
    let (_, mut cols, _) = batch.into_parts();
    cols[ti] = std::sync::Arc::new(new_ts) as arrow_array::ArrayRef;
    RecordBatch::try_new(new_schema, cols)
}

/// Resolve `output_tz` to an IANA name when relabeling is needed (`None` = keep UTC metadata).
pub(crate) async fn resolve_output_tz_iana(
    engine: &Engine,
    params: &RequestParams,
) -> Result<Option<String>, BlpAsyncError> {
    if !intraday_operation(params) {
        return Ok(None);
    }
    let label = match params.output_tz.as_deref() {
        None | Some("") => return Ok(None),
        Some(s) if s.eq_ignore_ascii_case("utc") => return Ok(None),
        Some(s) => s,
    };
    let iana = resolve_tz_label(engine, label, params.security.as_deref()).await?;
    if iana == "UTC" {
        return Ok(None);
    }
    Ok(Some(iana))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rfc3339_utc() {
        let u = parse_user_datetime("2024-06-01T14:30:00+00:00").unwrap();
        match u {
            UserDateTime::Utc(dt) => assert_eq!(dt.timestamp(), 1717252200),
            UserDateTime::Naive(_) => panic!("expected utc"),
        }
    }

    #[test]
    fn wall_utc_identity() {
        let s = wall_to_utc_iso("2024-06-01 14:30:00", "UTC").unwrap();
        assert!(s.starts_with("2024-06-01T14:30"));
    }

    #[test]
    fn hk_wall_time_converts_to_previous_utc_day() {
        let s = wall_to_utc_iso("2026-04-28 06:00", "Asia/Hong_Kong").unwrap();
        assert_eq!(s, "2026-04-27T22:00:00");
    }

    #[test]
    fn aware_input_preserves_instant() {
        let s = wall_to_utc_iso("2026-04-28T06:00:00+08:00", "America/New_York").unwrap();
        assert_eq!(s, "2026-04-27T22:00:00");
    }

    #[test]
    fn raw_intraday_request_operation_is_intraday() {
        let params = RequestParams {
            operation: Operation::RawRequest.to_string(),
            request_operation: Some(Operation::IntradayBar.to_string()),
            ..Default::default()
        };
        assert!(intraday_operation(&params));
    }
}
