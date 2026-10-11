use std::collections::HashMap;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

/// Market rule loaded from TOML. Defines how to derive sessions from regular trading hours.
#[derive(Debug, Clone, PartialEq, Default, Deserialize, Serialize)]
pub struct MarketRule {
    #[serde(default)]
    pub pre_minutes: i32,
    #[serde(default)]
    pub post_minutes: i32,
    #[serde(default)]
    pub lunch_start_min: Option<i32>,
    #[serde(default)]
    pub lunch_end_min: Option<i32>,
    #[serde(default)]
    pub day_start: Option<String>,
    #[serde(default)]
    pub day_end: Option<String>,
    #[serde(default)]
    pub is_continuous: bool,
}

/// Derived trading session windows. Times are "HH:MM" strings.
#[derive(Debug, Clone, PartialEq, Default, Deserialize, Serialize)]
pub struct SessionWindows {
    pub day: Option<(String, String)>,
    pub allday: Option<(String, String)>,
    pub pre: Option<(String, String)>,
    pub post: Option<(String, String)>,
    pub am: Option<(String, String)>,
    pub pm: Option<(String, String)>,
}

// The TOML is embedded at compile time. It lives inside this crate rather than in
// the repo-root `defs/` because `include_str!` may not reach outside the package
// root: an out-of-crate path publishes an sdist that cannot build.
const EXCHANGES_TOML: &str = include_str!("../../data/exchanges.toml");

#[derive(Deserialize)]
struct ExchangesToml {
    #[serde(default)]
    mic: HashMap<String, MarketRule>,
    #[serde(default)]
    exch_code: HashMap<String, MarketRule>,
    #[serde(default)]
    country_tz: HashMap<String, String>,
}

static EXCHANGES: OnceLock<Result<ExchangesToml, toml::de::Error>> = OnceLock::new();

fn exchanges() -> Option<&'static ExchangesToml> {
    EXCHANGES
        .get_or_init(|| toml::from_str(EXCHANGES_TOML))
        .as_ref()
        .ok()
}

/// Look up a MarketRule by MIC code, then fallback to exchange code.
pub fn get_market_rule(mic: Option<&str>, exch_code: Option<&str>) -> Option<&'static MarketRule> {
    let data = exchanges()?;
    if let Some(mic_code) = mic.map(str::trim).filter(|s| !s.is_empty())
        && let Some(rule) = data.mic.get(mic_code)
    {
        return Some(rule);
    }
    if let Some(code) = exch_code.map(str::trim).filter(|s| !s.is_empty())
        && let Some(rule) = data.exch_code.get(code)
    {
        return Some(rule);
    }
    None
}

/// Infer IANA timezone from country ISO code.
pub fn infer_timezone_from_country(country_iso: &str) -> Option<&'static str> {
    let key = country_iso.trim().to_uppercase();
    exchanges()?.country_tz.get(&key).map(String::as_str)
}

/// Derive session windows from regular trading hours and a market rule.
/// `day_start` and `day_end` are "HH:MM" strings from Bloomberg.
pub fn derive_sessions(
    day_start: &str,
    day_end: &str,
    mic: Option<&str>,
    exch_code: Option<&str>,
) -> SessionWindows {
    let Some(input_day_start_tm) = parse_time(day_start) else {
        return SessionWindows::default();
    };
    let Some(input_day_end_tm) = parse_time(day_end) else {
        return SessionWindows::default();
    };

    let rule = get_market_rule(mic, exch_code);
    let day_start_tm = rule
        .and_then(|rule| rule.day_start.as_deref())
        .and_then(parse_time)
        .unwrap_or(input_day_start_tm);
    let day_end_tm = rule
        .and_then(|rule| rule.day_end.as_deref())
        .and_then(parse_time)
        .unwrap_or(input_day_end_tm);

    let day_start_str = format_time(day_start_tm.0, day_start_tm.1);
    let day_end_str = format_time(day_end_tm.0, day_end_tm.1);

    let mut windows = SessionWindows {
        day: Some((day_start_str.clone(), day_end_str.clone())),
        ..SessionWindows::default()
    };

    if let Some(rule) = rule {
        if rule.is_continuous {
            windows.allday = windows.day.clone();
            return windows;
        }

        if rule.pre_minutes > 0 {
            let pre_start = add_minutes(day_start_tm, -rule.pre_minutes);
            windows.pre = Some((format_time(pre_start.0, pre_start.1), day_start_str.clone()));
        }

        if rule.post_minutes > 0 {
            let post_start = add_minutes(day_end_tm, 1);
            let post_end = add_minutes(day_end_tm, rule.post_minutes);
            windows.post = Some((
                format_time(post_start.0, post_start.1),
                format_time(post_end.0, post_end.1),
            ));
        }

        let allday_start = windows
            .pre
            .as_ref()
            .map(|(start, _)| start.clone())
            .unwrap_or_else(|| day_start_str.clone());
        let allday_end = windows
            .post
            .as_ref()
            .map(|(_, end)| end.clone())
            .unwrap_or_else(|| day_end_str.clone());
        windows.allday = Some((allday_start, allday_end));

        if let (Some(lunch_start), Some(lunch_end)) = (rule.lunch_start_min, rule.lunch_end_min) {
            let am_end = add_minutes(day_start_tm, lunch_start);
            let pm_start = add_minutes(day_start_tm, lunch_end);
            windows.am = Some((day_start_str, format_time(am_end.0, am_end.1)));
            windows.pm = Some((format_time(pm_start.0, pm_start.1), day_end_str));
        }
    } else {
        windows.allday = windows.day.clone();
    }

    windows
}

fn parse_time(s: &str) -> Option<(i32, i32)> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let (h, m) = if let Some((hh, mm)) = s.split_once(':') {
        let hour = hh.parse::<i32>().ok()?;
        let minute = mm.get(..2).unwrap_or(mm).parse::<i32>().ok()?;
        (hour, minute)
    } else if s.len() == 4 && s.chars().all(|c| c.is_ascii_digit()) {
        let hour = s[..2].parse::<i32>().ok()?;
        let minute = s[2..4].parse::<i32>().ok()?;
        (hour, minute)
    } else {
        return None;
    };

    if (0..=23).contains(&h) && (0..=59).contains(&m) {
        Some((h, m))
    } else {
        None
    }
}

fn format_time(h: i32, m: i32) -> String {
    format!("{:02}:{:02}", h.rem_euclid(24), m.rem_euclid(60))
}

fn add_minutes(time: (i32, i32), mins: i32) -> (i32, i32) {
    let total = (time.0 * 60 + time.1 + mins).rem_euclid(24 * 60);
    (total / 60, total % 60)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_toml() {
        let rule = get_market_rule(Some("XNYS"), None);
        assert!(rule.is_some());
        let rule = rule.unwrap();
        assert_eq!(rule.pre_minutes, 330);
        assert_eq!(rule.post_minutes, 240);
    }

    #[test]
    fn test_derive_sessions_us_equity() {
        let sw = derive_sessions("09:30", "16:00", Some("XNYS"), Some("US"));
        assert_eq!(sw.day, Some(("09:30".to_string(), "16:00".to_string())));
        assert_eq!(sw.pre, Some(("04:00".to_string(), "09:30".to_string())));
        assert_eq!(sw.post, Some(("16:01".to_string(), "20:00".to_string())));
        assert_eq!(sw.allday, Some(("04:00".to_string(), "20:00".to_string())));
    }

    #[test]
    fn test_derive_sessions_japan() {
        let sw = derive_sessions("09:00", "15:00", Some("XTKS"), Some("JT"));
        assert_eq!(sw.day, Some(("09:00".to_string(), "15:30".to_string())));
        assert_eq!(sw.am, Some(("09:00".to_string(), "11:30".to_string())));
        assert_eq!(sw.pm, Some(("12:30".to_string(), "15:30".to_string())));
        assert_eq!(sw.pre, Some(("08:00".to_string(), "09:00".to_string())));
        assert_eq!(sw.post, Some(("15:31".to_string(), "16:00".to_string())));
    }

    #[test]
    fn test_derive_sessions_futures() {
        let sw = derive_sessions("18:00", "17:00", Some("XCME"), Some("CME"));
        assert_eq!(sw.day, Some(("18:00".to_string(), "17:00".to_string())));
        assert_eq!(sw.allday, sw.day);
        assert_eq!(sw.pre, None);
        assert_eq!(sw.post, None);
        assert_eq!(sw.am, None);
        assert_eq!(sw.pm, None);
    }

    #[test]
    fn test_derive_sessions_no_rule() {
        let sw = derive_sessions("09:30", "16:00", Some("XXXX"), Some("YY"));
        assert_eq!(sw.day, Some(("09:30".to_string(), "16:00".to_string())));
        assert_eq!(sw.allday, sw.day);
        assert_eq!(sw.pre, None);
        assert_eq!(sw.post, None);
    }

    #[test]
    fn test_infer_timezone() {
        assert_eq!(infer_timezone_from_country("US"), Some("America/New_York"));
        assert_eq!(infer_timezone_from_country("JP"), Some("Asia/Tokyo"));
    }

    #[test]
    fn test_time_arithmetic() {
        assert_eq!(add_minutes((23, 50), 20), (0, 10));
        assert_eq!(add_minutes((0, 10), -20), (23, 50));
        assert_eq!(add_minutes((9, 30), -330), (4, 0));
    }
}
