//! Date parsing and formatting utilities.
//!
//! Provides fast date parsing with support for multiple common formats.

use chrono::NaiveDate;

use crate::error::{ExtError, Result};

/// Supported date formats for parsing.
const DATE_FORMATS: &[&str] = &[
    "%Y-%m-%d", // 2024-01-15
    "%Y%m%d",   // 20240115
    "%Y/%m/%d", // 2024/01/15
    "%d-%m-%Y", // 15-01-2024
    "%d/%m/%Y", // 15/01/2024
];

/// Parse a date string into a `NaiveDate`.
///
/// Supports multiple formats:
/// - `YYYY-MM-DD` (ISO 8601)
/// - `YYYYMMDD` (Bloomberg compact)
/// - `YYYY/MM/DD`
/// - `DD-MM-YYYY`
/// - `DD/MM/YYYY`
///
/// # Examples
///
/// ```
/// use xbbg_ext::utils::date::parse_date;
///
/// let d1 = parse_date("2024-01-15").unwrap();
/// let d2 = parse_date("20240115").unwrap();
/// let d3 = parse_date("2024/01/15").unwrap();
///
/// assert_eq!(d1, d2);
/// assert_eq!(d2, d3);
/// ```
///
/// # Errors
///
/// Returns `ExtError::DateParse` if the string doesn't match any supported format.
pub fn parse_date(s: &str) -> Result<NaiveDate> {
    let s = s.trim();

    // Fast path: check length to narrow down formats
    match s.len() {
        8 => {
            // YYYYMMDD format
            if let Ok(d) = NaiveDate::parse_from_str(s, "%Y%m%d") {
                return Ok(d);
            }
        }
        10 => {
            // Most common: YYYY-MM-DD or YYYY/MM/DD
            if s.as_bytes()[4] == b'-' {
                if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    return Ok(d);
                }
            } else if s.as_bytes()[4] == b'/' {
                if let Ok(d) = NaiveDate::parse_from_str(s, "%Y/%m/%d") {
                    return Ok(d);
                }
            } else if s.as_bytes()[2] == b'-' {
                if let Ok(d) = NaiveDate::parse_from_str(s, "%d-%m-%Y") {
                    return Ok(d);
                }
            } else if s.as_bytes()[2] == b'/'
                && let Ok(d) = NaiveDate::parse_from_str(s, "%d/%m/%Y")
            {
                return Ok(d);
            }
        }
        _ => {}
    }

    // Fallback: try all formats
    for fmt in DATE_FORMATS {
        if let Ok(d) = NaiveDate::parse_from_str(s, fmt) {
            return Ok(d);
        }
    }

    Err(ExtError::DateParse(s.to_string()))
}

/// Format a date to a string.
///
/// # Arguments
///
/// * `date` - The date to format
/// * `fmt` - Optional format string (default: `%Y%m%d` for Bloomberg)
///
/// # Examples
///
/// ```
/// use chrono::NaiveDate;
/// use xbbg_ext::utils::date::fmt_date;
///
/// let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
///
/// assert_eq!(fmt_date(d, None), "20240115");
/// assert_eq!(fmt_date(d, Some("%Y-%m-%d")), "2024-01-15");
/// ```
pub fn fmt_date(date: NaiveDate, fmt: Option<&str>) -> String {
    date.format(fmt.unwrap_or("%Y%m%d")).to_string()
}

/// Try to parse a date, returning None if parsing fails.
///
/// Useful when date parsing is optional or for filter operations.
pub fn try_parse_date(s: &str) -> Option<NaiveDate> {
    parse_date(s).ok()
}

/// Compute default date range for turnover queries.
///
/// Returns `(start_date, end_date)` as ISO-8601 strings.
/// * `end_date` defaults to yesterday if not provided.
/// * `start_date` defaults to 30 days before `end_date` if not provided.
///
/// # Examples
///
/// ```
/// use xbbg_ext::utils::date::default_turnover_dates;
///
/// let (start, end) = default_turnover_dates(None, None);
/// assert_eq!(start.len(), 10); // "YYYY-MM-DD"
/// assert_eq!(end.len(), 10);
///
/// let (start2, end2) = default_turnover_dates(None, Some("2024-06-15"));
/// assert_eq!(end2, "2024-06-15");
/// assert_eq!(start2, "2024-05-16");
/// ```
pub fn default_turnover_dates(
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> (String, String) {
    let end = match end_date {
        Some(s) => try_parse_date(s).unwrap_or_else(|| {
            chrono::Local::now().naive_local().date() - chrono::Duration::days(1)
        }),
        None => chrono::Local::now().naive_local().date() - chrono::Duration::days(1),
    };

    let start = match start_date {
        Some(s) => try_parse_date(s).unwrap_or(end - chrono::Duration::days(30)),
        None => end - chrono::Duration::days(30),
    };

    (
        fmt_date(start, Some("%Y-%m-%d")),
        fmt_date(end, Some("%Y-%m-%d")),
    )
}

/// Compute default datetime range for BQR (quote request) queries.
///
/// Returns `(start_datetime, end_datetime)` as ISO-8601 datetime strings.
/// * `end_datetime` defaults to now if not provided.
/// * `start_datetime` defaults to 1 hour before `end_datetime` if not provided.
///
/// Input datetimes support both `YYYY-MM-DD HH:MM` and `YYYY-MM-DDTHH:MM` formats.
///
/// # Examples
///
/// ```
/// use xbbg_ext::utils::date::default_bqr_datetimes;
///
/// let (start, end) = default_bqr_datetimes(None, None);
/// assert!(start.contains('T'));
/// assert!(end.contains('T'));
///
/// let (start2, end2) = default_bqr_datetimes(
///     Some("2024-01-15 09:00"),
///     Some("2024-01-15 10:00"),
/// );
/// assert_eq!(start2, "2024-01-15T09:00:00");
/// assert_eq!(end2, "2024-01-15T10:00:00");
/// ```
pub fn default_bqr_datetimes(
    start_datetime: Option<&str>,
    end_datetime: Option<&str>,
) -> (String, String) {
    let end_str = match end_datetime {
        Some(s) => normalize_datetime_str(s),
        None => chrono::Local::now()
            .naive_local()
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string(),
    };

    let start_str = match start_datetime {
        Some(s) => normalize_datetime_str(s),
        None => {
            // Parse end_str back to compute 1 hour before
            let end_dt = chrono::NaiveDateTime::parse_from_str(&end_str, "%Y-%m-%dT%H:%M:%S")
                .unwrap_or_else(|_| chrono::Local::now().naive_local());
            let start_dt = end_dt - chrono::Duration::hours(1);
            start_dt.format("%Y-%m-%dT%H:%M:%S").to_string()
        }
    };

    (start_str, end_str)
}

/// Normalize a datetime string to ISO-8601 format `YYYY-MM-DDTHH:MM:SS`.
///
/// Handles:
/// * `YYYY-MM-DD HH:MM` → `YYYY-MM-DDTHH:MM:00`
/// * `YYYY-MM-DDTHH:MM` → `YYYY-MM-DDTHH:MM:00`
/// * Already complete strings pass through.
fn normalize_datetime_str(s: &str) -> String {
    let s = s.replace(' ', "T");
    if s.len() == 16 && s.contains('T') {
        // YYYY-MM-DDTHH:MM → add :00
        format!("{}:00", s)
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_parse_iso() {
        let d = parse_date("2024-01-15").unwrap();
        assert_eq!(d.year(), 2024);
        assert_eq!(d.month(), 1);
        assert_eq!(d.day(), 15);
    }

    #[test]
    fn test_parse_compact() {
        let d = parse_date("20240115").unwrap();
        assert_eq!(d.year(), 2024);
        assert_eq!(d.month(), 1);
        assert_eq!(d.day(), 15);
    }

    #[test]
    fn test_parse_slash() {
        let d = parse_date("2024/01/15").unwrap();
        assert_eq!(d.year(), 2024);
        assert_eq!(d.month(), 1);
        assert_eq!(d.day(), 15);
    }

    #[test]
    fn test_parse_euro_dash() {
        let d = parse_date("15-01-2024").unwrap();
        assert_eq!(d.year(), 2024);
        assert_eq!(d.month(), 1);
        assert_eq!(d.day(), 15);
    }

    #[test]
    fn test_parse_euro_slash() {
        let d = parse_date("15/01/2024").unwrap();
        assert_eq!(d.year(), 2024);
        assert_eq!(d.month(), 1);
        assert_eq!(d.day(), 15);
    }

    #[test]
    fn test_parse_invalid() {
        assert!(parse_date("not-a-date").is_err());
        assert!(parse_date("").is_err());
        assert!(parse_date("2024").is_err());
    }

    #[test]
    fn test_parse_with_whitespace() {
        let d = parse_date("  2024-01-15  ").unwrap();
        assert_eq!(d.year(), 2024);
    }

    #[test]
    fn test_fmt_date_default() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        assert_eq!(fmt_date(d, None), "20240115");
    }

    #[test]
    fn test_fmt_date_custom() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        assert_eq!(fmt_date(d, Some("%Y-%m-%d")), "2024-01-15");
    }

    #[test]
    fn test_all_formats_same_result() {
        let dates = [
            "2024-01-15",
            "20240115",
            "2024/01/15",
            "15-01-2024",
            "15/01/2024",
        ];

        let expected = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        for s in dates {
            assert_eq!(parse_date(s).unwrap(), expected, "Failed for: {}", s);
        }
    }
}
