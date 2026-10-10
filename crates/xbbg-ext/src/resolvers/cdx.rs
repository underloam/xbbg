//! CDX (Credit Default Index) ticker resolution utilities.
use std::num::NonZeroU32;

use chrono::{Months, NaiveDate};

use crate::error::{ExtError, Result};

/// CDX ticker series information.
///
/// This source-compatible view intentionally omits explicit version state.
/// Use [`parse_cdx_ticker`] when the input's `Vn` token matters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdxInfo {
    /// The index name (e.g., "CDX IG CDSI")
    pub index: String,
    /// Series indicator - either "GEN" or "S{n}" (e.g., "S45")
    pub series: String,
    /// Tenor (e.g., "5Y")
    pub tenor: String,
    /// Asset class (e.g., "Corp")
    pub asset: String,
    /// Whether this is a generic ticker
    pub is_generic: bool,
    /// Series number if specific (None for GEN)
    pub series_num: Option<u32>,
}

/// Syntactically parsed CDX ticker, including an optional explicit version.
///
/// `version` is `None` for a versionless Bloomberg alias. That state is
/// unresolved and must never be interpreted as V1 without Bloomberg metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCdxInfo {
    /// The index name (e.g., "CDX IG CDSI")
    pub index: String,
    /// Series indicator - either "GEN" or "S{n}" (e.g., "S45")
    pub series: String,
    /// Explicit version token, when present in the input ticker
    pub version: Option<CdxVersion>,
    /// Tenor (e.g., "5Y")
    pub tenor: String,
    /// Asset class (e.g., "Corp")
    pub asset: String,
    /// Whether this is a generic ticker
    pub is_generic: bool,
    /// Series number if specific (None for GEN)
    pub series_num: Option<u32>,
}

impl From<ParsedCdxInfo> for CdxInfo {
    fn from(parsed: ParsedCdxInfo) -> Self {
        Self {
            index: parsed.index,
            series: parsed.series,
            tenor: parsed.tenor,
            asset: parsed.asset,
            is_generic: parsed.is_generic,
            series_num: parsed.series_num,
        }
    }
}

/// Positive CDX contract version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CdxVersion(NonZeroU32);

impl CdxVersion {
    /// Construct a positive CDX version.
    pub fn new(value: u32) -> Result<Self> {
        NonZeroU32::new(value)
            .map(Self)
            .ok_or_else(|| ExtError::InvalidInput("CDX version must be positive".to_string()))
    }

    /// Return the numeric version.
    pub fn get(self) -> u32 {
        self.0.get()
    }
}

/// Fully resolved specific CDX contract.
///
/// Unlike [`ParsedCdxInfo`], this type cannot represent a generic or
/// versionless ticker. Construct it only after resolving both series and
/// version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedCdxInfo {
    index: String,
    series: NonZeroU32,
    version: CdxVersion,
    tenor: String,
    asset: String,
}

impl ResolvedCdxInfo {
    /// Resolve parsed ticker information with Bloomberg series/version metadata.
    pub fn resolve(parsed: ParsedCdxInfo, series: u32, version: u32) -> Result<Self> {
        let series = NonZeroU32::new(series)
            .ok_or_else(|| ExtError::InvalidInput("CDX series must be positive".to_string()))?;
        let version = CdxVersion::new(version)?;

        if !parsed.is_generic && parsed.series_num != Some(series.get()) {
            return Err(ExtError::InvalidInput(format!(
                "resolved CDX series S{} conflicts with parsed {}",
                series.get(),
                parsed.series
            )));
        }
        if parsed
            .version
            .is_some_and(|parsed_version| parsed_version != version)
        {
            return Err(ExtError::InvalidInput(format!(
                "resolved CDX version V{} conflicts with parsed ticker",
                version.get()
            )));
        }

        Ok(Self {
            index: parsed.index,
            series,
            version,
            tenor: parsed.tenor,
            asset: parsed.asset,
        })
    }

    /// Numeric series.
    pub fn series(&self) -> u32 {
        self.series.get()
    }

    /// Numeric version.
    pub fn version(&self) -> u32 {
        self.version.get()
    }

    /// Versionless alias for the preceding series, whose version is unresolved.
    pub fn previous_alias(&self) -> Option<String> {
        (self.series.get() > 1).then(|| {
            format!(
                "{} S{} {} {}",
                self.index,
                self.series.get() - 1,
                self.tenor,
                self.asset
            )
        })
    }
}

/// Parse a CDX ticker into its source-compatible series view.
///
/// # Examples
///
/// ```
/// use xbbg_ext::resolvers::cdx::cdx_series_from_ticker;
///
/// let info = cdx_series_from_ticker("CDX IG CDSI GEN 5Y Corp").unwrap();
/// assert!(info.is_generic);
/// assert_eq!(info.series, "GEN");
/// assert_eq!(info.tenor, "5Y");
///
/// let info2 = cdx_series_from_ticker("CDX IG CDSI S45 5Y Corp").unwrap();
/// assert!(!info2.is_generic);
/// assert_eq!(info2.series_num, Some(45));
/// ```
pub fn cdx_series_from_ticker(ticker: &str) -> Result<CdxInfo> {
    parse_cdx_ticker(ticker).map(Into::into)
}

/// Parse a CDX ticker, preserving an optional explicit `Vn` token.
pub fn parse_cdx_ticker(ticker: &str) -> Result<ParsedCdxInfo> {
    let parts: Vec<&str> = ticker.split_whitespace().collect();

    if parts.len() < 5 {
        return Err(ExtError::InvalidTicker(ticker.to_string()));
    }

    let mut series_idx = None;
    let mut series = String::new();
    let mut is_generic = false;
    let mut series_num = None;

    for (i, part) in parts.iter().enumerate() {
        if *part == "GEN" {
            series_idx = Some(i);
            series = "GEN".to_string();
            is_generic = true;
            break;
        }
        if let Some(num_part) = part.strip_prefix('S') {
            if let Ok(n) = num_part.parse::<u32>() {
                if n == 0 {
                    return Err(ExtError::InvalidTicker(ticker.to_string()));
                }
                series_idx = Some(i);
                series = part.to_string();
                series_num = Some(n);
                break;
            }
        }
    }

    let series_idx = series_idx.ok_or_else(|| ExtError::InvalidTicker(ticker.to_string()))?;
    let index = parts[..series_idx].join(" ");
    let mut next_idx = series_idx + 1;

    let version = if let Some(token) = parts.get(next_idx).filter(|token| token.starts_with('V')) {
        let raw_version = token
            .strip_prefix('V')
            .and_then(|value| value.parse::<u32>().ok())
            .ok_or_else(|| ExtError::InvalidTicker(ticker.to_string()))?;
        next_idx += 1;
        Some(
            CdxVersion::new(raw_version)
                .map_err(|_| ExtError::InvalidTicker(ticker.to_string()))?,
        )
    } else {
        None
    };

    if next_idx + 2 != parts.len() {
        return Err(ExtError::InvalidTicker(ticker.to_string()));
    }

    Ok(ParsedCdxInfo {
        index,
        series,
        version,
        tenor: parts[next_idx].to_string(),
        asset: parts[next_idx + 1].to_string(),
        is_generic,
        series_num,
    })
}

/// Format a resolved CDX contract, optionally omitting its version token.
///
/// `versionless` is presentation-only; the resolved value retains its version.
pub fn build_resolved_cdx_ticker_with_options(info: &ResolvedCdxInfo, versionless: bool) -> String {
    if versionless {
        format!(
            "{} S{} {} {}",
            info.index,
            info.series.get(),
            info.tenor,
            info.asset
        )
    } else {
        format!(
            "{} S{} V{} {} {}",
            info.index,
            info.series.get(),
            info.version.get(),
            info.tenor,
            info.asset
        )
    }
}

fn build_cdx_alias(info: &ParsedCdxInfo) -> String {
    match info.version {
        Some(version) => format!(
            "{} {} V{} {} {}",
            info.index,
            info.series,
            version.get(),
            info.tenor,
            info.asset
        ),
        None => format!(
            "{} {} {} {}",
            info.index, info.series, info.tenor, info.asset
        ),
    }
}

/// Get the previous series ticker for a CDX index.
///
/// # Examples
///
/// ```
/// use xbbg_ext::resolvers::cdx::previous_series_ticker;
///
/// let prev = previous_series_ticker("CDX IG CDSI S45 5Y Corp").unwrap();
/// assert!(prev.is_some());
/// assert!(prev.unwrap().contains("S44"));
///
/// // GEN doesn't have a previous series
/// let prev_gen = previous_series_ticker("CDX IG CDSI GEN 5Y Corp").unwrap();
/// assert!(prev_gen.is_none());
/// ```
pub fn previous_series_ticker(ticker: &str) -> Result<Option<String>> {
    let mut info = parse_cdx_ticker(ticker)?;

    if info.is_generic {
        return Ok(None);
    }

    match info.series_num {
        Some(n) if n > 1 => {
            info.series = format!("S{}", n - 1);
            info.series_num = Some(n - 1);
            info.version = None;
            Ok(Some(build_cdx_alias(&info)))
        }
        _ => Ok(None),
    }
}

/// Convert a generic CDX ticker to a specific series.
///
/// # Examples
///
/// ```
/// use xbbg_ext::resolvers::cdx::gen_to_specific;
///
/// let specific = gen_to_specific("CDX IG CDSI GEN 5Y Corp", 45).unwrap();
/// assert!(specific.contains("S45"));
/// assert!(!specific.contains("GEN"));
/// ```
pub fn gen_to_specific(gen_ticker: &str, series: u32) -> Result<String> {
    let mut info = parse_cdx_ticker(gen_ticker)?;
    if !info.is_generic {
        return Err(ExtError::SpecificTicker(gen_ticker.to_string()));
    }
    if series == 0 {
        return Err(ExtError::InvalidInput(
            "CDX series must be positive".to_string(),
        ));
    }

    info.series = format!("S{series}");
    info.series_num = Some(series);
    info.version = None;
    info.is_generic = false;

    Ok(build_cdx_alias(&info))
}

/// Months between consecutive CDS index series.
///
/// Markit rolls CDX and iTraxx semi-annually, nominally on 20 March and
/// 20 September. The effective date is business-day adjusted (CDX.NA.IG.45
/// starts 2025-09-22, not 2025-09-20), so this cadence may only size a search
/// window -- never decide which series applies.
const CDX_ROLL_MONTHS: u32 = 6;

/// Maximum prior series probed per ladder batch.
///
/// Twelve years of semi-annual rolls. Callers that exhaust a batch without a
/// match continue with the next block down.
pub const CDX_LADDER_BATCH: u32 = 24;

/// Estimated number of semi-annual rolls between `dt` and `accrual_start`.
///
/// Zero when `dt` is on or after `accrual_start`. The result is an estimate
/// derived from the nominal roll cadence and is only ever used to size a
/// candidate window; the series that applies to `dt` must still be decided by
/// comparing `dt` against the accrual dates Bloomberg reports.
///
/// # Examples
///
/// ```
/// use chrono::NaiveDate;
/// use xbbg_ext::resolvers::cdx::cdx_rolls_between;
///
/// let accrual = NaiveDate::from_ymd_opt(2026, 3, 20).unwrap();
/// let dt = NaiveDate::from_ymd_opt(2026, 3, 18).unwrap();
/// assert_eq!(cdx_rolls_between(dt, accrual), 1);
/// assert_eq!(cdx_rolls_between(accrual, accrual), 0);
/// ```
pub fn cdx_rolls_between(dt: NaiveDate, accrual_start: NaiveDate) -> u32 {
    let step = Months::new(CDX_ROLL_MONTHS);
    let mut probe = accrual_start;
    let mut rolls = 0;

    while probe > dt && rolls < CDX_LADDER_BATCH {
        let Some(earlier) = probe.checked_sub_months(step) else {
            break;
        };
        probe = earlier;
        rolls += 1;
    }

    rolls
}

/// Inclusive series window to probe first when `dt` precedes `current_series`.
///
/// `None` when `current_series` already covers `dt`, or when no prior series
/// exists. The window spans one series past the cadence estimate so a
/// business-day-adjusted roll cannot fall outside it.
///
/// # Examples
///
/// ```
/// use chrono::NaiveDate;
/// use xbbg_ext::resolvers::cdx::cdx_prior_series_window;
///
/// let accrual = NaiveDate::from_ymd_opt(2026, 3, 20).unwrap();
/// let dt = NaiveDate::from_ymd_opt(2026, 3, 18).unwrap();
/// assert_eq!(cdx_prior_series_window(46, accrual, dt), Some((44, 45)));
/// ```
pub fn cdx_prior_series_window(
    current_series: u32,
    current_accrual_start: NaiveDate,
    dt: NaiveDate,
) -> Option<(u32, u32)> {
    if dt >= current_accrual_start || current_series <= 1 {
        return None;
    }

    let hi = current_series - 1;
    let lo = hi
        .saturating_sub(cdx_rolls_between(dt, current_accrual_start))
        .max(1);
    Some((lo, hi))
}

/// Inclusive series window immediately below an exhausted one.
///
/// `None` once series 1 has been probed.
pub fn cdx_next_series_window(probed_lo: u32) -> Option<(u32, u32)> {
    if probed_lo <= 1 {
        return None;
    }

    let hi = probed_lo - 1;
    let lo = hi.saturating_sub(CDX_LADDER_BATCH - 1).max(1);
    Some((lo, hi))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn day(text: &str) -> NaiveDate {
        NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
    }

    #[test]
    fn test_cdx_rolls_between_counts_semiannual_steps() {
        // CDX.NA.IG.46 first accrues 2026-03-20.
        let accrual = day("2026-03-20");
        assert_eq!(cdx_rolls_between(accrual, accrual), 0);
        assert_eq!(cdx_rolls_between(day("2026-06-01"), accrual), 0);
        assert_eq!(cdx_rolls_between(day("2026-03-18"), accrual), 1);
        assert_eq!(cdx_rolls_between(day("2025-06-01"), accrual), 2);
        // S34 first accrues 2020-03-20, twelve rolls earlier.
        assert_eq!(cdx_rolls_between(day("2020-06-01"), accrual), 12);
    }

    #[test]
    fn test_cdx_rolls_between_is_capped_per_batch() {
        assert_eq!(
            cdx_rolls_between(day("1900-01-01"), day("2026-03-20")),
            CDX_LADDER_BATCH
        );
    }

    #[test]
    fn test_cdx_prior_series_window_spans_the_cadence_estimate() {
        let accrual = day("2026-03-20");
        // The window reaches one series past the estimate, so a roll adjusted
        // forward off the 20th cannot fall outside it.
        assert_eq!(
            cdx_prior_series_window(46, accrual, day("2026-03-18")),
            Some((44, 45))
        );
        assert_eq!(
            cdx_prior_series_window(46, accrual, day("2020-06-01")),
            Some((33, 45))
        );
    }

    #[test]
    fn test_cdx_prior_series_window_clamps_to_first_series() {
        let accrual = day("2026-03-20");
        assert_eq!(
            cdx_prior_series_window(3, accrual, day("2020-06-01")),
            Some((1, 2))
        );
        // The current series already covers the date, or has no predecessor.
        assert_eq!(cdx_prior_series_window(46, accrual, accrual), None);
        assert_eq!(cdx_prior_series_window(1, accrual, day("2020-06-01")), None);
    }

    #[test]
    fn test_cdx_next_series_window_walks_down_to_series_one() {
        assert_eq!(cdx_next_series_window(33), Some((9, 32)));
        assert_eq!(cdx_next_series_window(10), Some((1, 9)));
        assert_eq!(cdx_next_series_window(1), None);
    }

    #[test]
    fn test_parse_generic_cdx() {
        let info = parse_cdx_ticker("CDX IG CDSI GEN 5Y Corp").unwrap();
        assert_eq!(info.index, "CDX IG CDSI");
        assert_eq!(info.series, "GEN");
        assert_eq!(info.version, None);
        assert_eq!(info.tenor, "5Y");
        assert_eq!(info.asset, "Corp");
        assert!(info.is_generic);
        assert_eq!(info.series_num, None);
    }

    #[test]
    fn test_parse_specific_cdx_alias() {
        let info = parse_cdx_ticker("CDX HY CDSI S46 5Y Corp").unwrap();
        assert_eq!(info.series_num, Some(46));
        assert_eq!(info.version, None);
        assert_eq!(info.tenor, "5Y");
        assert!(!info.is_generic);
    }

    #[test]
    fn test_parse_specific_cdx_with_version() {
        let info = parse_cdx_ticker("CDX HY CDSI S46 V2 5Y Corp").unwrap();
        assert_eq!(info.series_num, Some(46));
        assert_eq!(info.version.map(CdxVersion::get), Some(2));
        assert_eq!(info.tenor, "5Y");
    }

    #[test]
    fn test_parse_five_token_legacy_alias() {
        let info = cdx_series_from_ticker("CDX IG S40 5Y Corp").unwrap();
        assert_eq!(info.index, "CDX IG");
        assert_eq!(info.series_num, Some(40));
        assert_eq!(info.tenor, "5Y");
        assert_eq!(
            previous_series_ticker("CDX IG S40 5Y Corp").unwrap(),
            Some("CDX IG S39 5Y Corp".to_string())
        );
    }

    #[test]
    fn test_resolved_ticker_version_presentation() {
        let parsed = parse_cdx_ticker("CDX IG CDSI GEN 5Y Corp").unwrap();
        let resolved = ResolvedCdxInfo::resolve(parsed, 46, 1).unwrap();

        assert_eq!(
            build_resolved_cdx_ticker_with_options(&resolved, false),
            "CDX IG CDSI S46 V1 5Y Corp"
        );
        assert_eq!(
            build_resolved_cdx_ticker_with_options(&resolved, true),
            "CDX IG CDSI S46 5Y Corp"
        );
        assert_eq!(resolved.series(), 46);
        assert_eq!(resolved.version(), 1);
    }

    #[test]
    fn test_previous_series_is_an_unresolved_alias() {
        let prev = previous_series_ticker("CDX HY CDSI S46 V2 5Y Corp")
            .unwrap()
            .unwrap();
        assert_eq!(prev, "CDX HY CDSI S45 5Y Corp");

        assert!(previous_series_ticker("CDX IG CDSI S1 V1 5Y Corp")
            .unwrap()
            .is_none());
        assert!(previous_series_ticker("CDX IG CDSI GEN 5Y Corp")
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_resolution_rejects_conflicting_explicit_version() {
        let parsed = parse_cdx_ticker("CDX HY CDSI S46 V2 5Y Corp").unwrap();
        assert!(ResolvedCdxInfo::resolve(parsed, 46, 1).is_err());
    }

    #[test]
    fn test_gen_to_specific_returns_unresolved_alias() {
        let specific = gen_to_specific("CDX IG CDSI GEN 5Y Corp", 45).unwrap();
        assert_eq!(specific, "CDX IG CDSI S45 5Y Corp");
        assert!(gen_to_specific("CDX IG CDSI S44 5Y Corp", 45).is_err());
        assert!(gen_to_specific("CDX IG CDSI GEN 5Y Corp", 0).is_err());
    }

    #[test]
    fn test_invalid_ticker() {
        assert!(parse_cdx_ticker("INVALID").is_err());
        assert!(parse_cdx_ticker("CDX IG").is_err());
        assert!(parse_cdx_ticker("CDX HY CDSI S0 V1 5Y Corp").is_err());
        assert!(parse_cdx_ticker("CDX HY CDSI S46 V0 5Y Corp").is_err());
        assert!(parse_cdx_ticker("CDX HY CDSI S46 VX 5Y Corp").is_err());
        assert!(parse_cdx_ticker("CDX HY CDSI S46 V2 V3 5Y Corp").is_err());
    }
}
