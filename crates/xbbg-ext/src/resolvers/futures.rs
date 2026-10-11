//! Futures ticker resolution utilities.
//!
//! Provides logic for generating futures contract candidates and resolving
//! generic tickers to specific contracts.

use chrono::{Datelike, Months, NaiveDate};

use crate::constants::{MONTH_NUM_TO_CODE, QUARTERLY_MONTHS};
use crate::error::{ExtError, Result};
use crate::utils::ticker::{TickerParts, is_specific_contract, parse_ticker_parts};

/// Roll frequency for futures contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollFrequency {
    /// Monthly contracts (all months)
    Monthly,
    /// Quarterly contracts (Mar, Jun, Sep, Dec)
    Quarterly,
}

impl std::str::FromStr for RollFrequency {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s.trim().to_uppercase().as_str() {
            "Q" | "QE" => RollFrequency::Quarterly,
            _ => RollFrequency::Monthly,
        })
    }
}

/// A futures contract candidate with its expected month.
#[derive(Debug, Clone)]
pub struct FuturesCandidate {
    /// The ticker string (e.g., "ESH24 Index")
    pub ticker: String,
    /// The contract month
    pub month: NaiveDate,
}

/// Generate futures contract candidates for a given generic ticker and date.
///
/// This generates the list of potential contract tickers that need to be
/// queried from Bloomberg to resolve the correct contract.
///
/// # Arguments
///
/// * `gen_ticker` - Generic futures ticker (e.g., "ES1 Index")
/// * `dt` - Reference date
/// * `freq` - Roll frequency (Monthly or Quarterly)
/// * `count` - Number of candidates to generate
///
/// # Returns
///
/// A vector of `FuturesCandidate` with ticker strings and their contract months.
///
/// # Examples
///
/// ```
/// use chrono::NaiveDate;
/// use xbbg_ext::resolvers::futures::{generate_futures_candidates, RollFrequency};
///
/// let dt = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
/// let candidates = generate_futures_candidates("ES1 Index", dt, RollFrequency::Quarterly, 4).unwrap();
///
/// assert!(!candidates.is_empty());
/// // First candidate should be March 2024 (ESH24 Index)
/// assert!(candidates[0].ticker.contains("H24") || candidates[0].ticker.contains("H4"));
/// ```
pub fn generate_futures_candidates(
    gen_ticker: &str,
    dt: NaiveDate,
    freq: RollFrequency,
    count: usize,
) -> Result<Vec<FuturesCandidate>> {
    // Validate it's a generic ticker
    if is_specific_contract(gen_ticker) {
        return Err(ExtError::SpecificTicker(gen_ticker.to_string()));
    }

    let parts = parse_ticker_parts(gen_ticker)?;

    // Generate contract months
    let months = generate_contract_months(dt, freq, count)?;

    // Check if we're in the same month as reference date
    let now = chrono::Local::now().naive_local().date();
    let same_month = now.month() == dt.month() && now.year() == dt.year();

    // Build candidate tickers
    let candidates = months
        .into_iter()
        .map(|month| {
            let ticker = build_contract_ticker(&parts, month, same_month);
            FuturesCandidate { ticker, month }
        })
        .collect();

    Ok(candidates)
}

/// Generate contract months starting from a given date.
fn generate_contract_months(
    start: NaiveDate,
    freq: RollFrequency,
    count: usize,
) -> Result<Vec<NaiveDate>> {
    let mut months = Vec::with_capacity(count);
    let mut current = start.with_day(1).ok_or_else(|| {
        ExtError::Internal(format!("failed to normalize contract month from {start}"))
    })?;

    match freq {
        RollFrequency::Monthly => {
            while months.len() < count {
                months.push(current);
                current = next_month(current)?;
            }
        }
        RollFrequency::Quarterly => {
            // Find next quarterly month
            while months.len() < count {
                if QUARTERLY_MONTHS.contains(&current.month()) {
                    months.push(current);
                }
                current = next_month(current)?;
            }
        }
    }

    Ok(months)
}

/// Get the next month from a date.
fn next_month(date: NaiveDate) -> Result<NaiveDate> {
    date.checked_add_months(Months::new(1))
        .and_then(|next| next.with_day(1))
        .ok_or_else(|| ExtError::Internal(format!("failed to compute next month from {date}")))
}

/// Build a specific contract ticker from parts and month.
fn build_contract_ticker(parts: &TickerParts, month: NaiveDate, same_month: bool) -> String {
    let month_code = MONTH_NUM_TO_CODE.get(&month.month()).unwrap_or(&"F");
    let year = month.year();

    // Use 1-digit year if same month, otherwise 2-digit
    let year_str = if same_month {
        format!("{}", year % 10)
    } else {
        format!("{:02}", year % 100)
    };

    match parts.asset.as_str() {
        "Equity" => {
            let exchange = parts.exchange.as_deref().unwrap_or("US");
            format!(
                "{}{}{} {} {}",
                parts.prefix, month_code, year_str, exchange, parts.asset
            )
        }
        _ => {
            format!("{}{}{} {}", parts.prefix, month_code, year_str, parts.asset)
        }
    }
}

/// Validate that a ticker is generic (not specific).
///
/// # Errors
///
/// Returns `ExtError::SpecificTicker` if the ticker appears to be a specific contract.
pub fn validate_generic_ticker(ticker: &str) -> Result<()> {
    if is_specific_contract(ticker) {
        Err(ExtError::SpecificTicker(ticker.to_string()))
    } else {
        Ok(())
    }
}

/// Extract the contract index from a generic ticker (e.g., "ES1 Index" -> 1).
///
/// Returns 0-based index (so "ES1" returns 0, "ES2" returns 1).
pub fn contract_index(gen_ticker: &str) -> Result<usize> {
    let parts = parse_ticker_parts(gen_ticker)?;
    Ok((parts.index as usize).saturating_sub(1))
}

/// Filter and sort futures contracts by maturity date.
///
/// Given a list of (ticker, maturity_date_str) pairs and a reference date,
/// keeps only contracts whose maturity falls after the reference date
/// and returns them sorted by maturity date ascending.
///
/// # Arguments
///
/// * `contracts` - Slice of (ticker, maturity_date_string) pairs
/// * `ref_date` - Reference date; contracts maturing on or before this are excluded
///
/// # Returns
///
/// Sorted list of ticker strings for contracts maturing after `ref_date`.
///
/// # Examples
///
/// ```
/// use chrono::NaiveDate;
/// use xbbg_ext::resolvers::futures::filter_valid_contracts;
///
/// let contracts = vec![
///     ("ESH24 Index".to_string(), "2024-03-15".to_string()),
///     ("ESM24 Index".to_string(), "2024-06-21".to_string()),
///     ("ESZ23 Index".to_string(), "2023-12-15".to_string()),
/// ];
/// let ref_date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
///
/// let valid = filter_valid_contracts(&contracts, ref_date);
/// assert_eq!(valid.len(), 2);
/// assert_eq!(valid[0], "ESH24 Index");
/// assert_eq!(valid[1], "ESM24 Index");
/// ```
pub fn filter_valid_contracts(contracts: &[(String, String)], ref_date: NaiveDate) -> Vec<String> {
    let mut valid: Vec<(String, NaiveDate)> = contracts
        .iter()
        .filter_map(|(ticker, matu_str)| {
            crate::utils::date::try_parse_date(matu_str).and_then(|matu_dt| {
                if matu_dt > ref_date {
                    Some((ticker.clone(), matu_dt))
                } else {
                    None
                }
            })
        })
        .collect();

    valid.sort_by_key(|(_, dt)| *dt);
    valid.into_iter().map(|(ticker, _)| ticker).collect()
}

/// Filter futures candidates by a cycle-months string.
///
/// Bloomberg's `FUT_GEN_MONTH` field returns a string of month codes
/// (e.g., `"HMUZ"` for quarterly, `"FHKNUX"` for grains).  This function
/// keeps only candidates whose contract month maps to a code present in
/// `cycle`.
///
/// # Arguments
///
/// * `candidates` – Slice of `(ticker, year, month)` tuples as returned by
///   `generate_futures_candidates` (via the pyo3 binding).
/// * `cycle` – Month-code string from Bloomberg (e.g., `"HMUZ"`).
///
/// # Returns
///
/// Filtered vector preserving the original order.
///
/// # Examples
///
/// ```
/// use xbbg_ext::resolvers::futures::filter_candidates_by_cycle;
///
/// let candidates = vec![
///     ("ESF24 Index".to_string(), 2024, 1u32),   // F = Jan
///     ("ESH24 Index".to_string(), 2024, 3u32),   // H = Mar
///     ("ESJ24 Index".to_string(), 2024, 4u32),   // J = Apr
///     ("ESM24 Index".to_string(), 2024, 6u32),   // M = Jun
/// ];
///
/// let filtered = filter_candidates_by_cycle(&candidates, "HMUZ");
/// assert_eq!(filtered.len(), 2);
/// assert_eq!(filtered[0].0, "ESH24 Index");
/// assert_eq!(filtered[1].0, "ESM24 Index");
/// ```
pub fn filter_candidates_by_cycle(
    candidates: &[(String, i32, u32)],
    cycle: &str,
) -> Vec<(String, i32, u32)> {
    let cycle_upper = cycle.to_uppercase();
    candidates
        .iter()
        .filter(|(_, _, month)| {
            MONTH_NUM_TO_CODE
                .get(month)
                .is_some_and(|code| cycle_upper.contains(code))
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_roll_frequency_parsing() {
        assert_eq!(
            RollFrequency::from_str("M").unwrap(),
            RollFrequency::Monthly
        );
        assert_eq!(
            RollFrequency::from_str("Q").unwrap(),
            RollFrequency::Quarterly
        );
        assert_eq!(
            RollFrequency::from_str("QE").unwrap(),
            RollFrequency::Quarterly
        );
        assert_eq!(RollFrequency::from_str("").unwrap(), RollFrequency::Monthly);
    }

    #[test]
    fn test_generate_monthly_candidates() {
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let candidates =
            generate_futures_candidates("ES1 Index", dt, RollFrequency::Monthly, 3).unwrap();

        assert_eq!(candidates.len(), 3);
        // Should start from January 2024
        assert_eq!(candidates[0].month.month(), 1);
        assert_eq!(candidates[1].month.month(), 2);
        assert_eq!(candidates[2].month.month(), 3);
    }

    #[test]
    fn test_generate_quarterly_candidates() {
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let candidates =
            generate_futures_candidates("ES1 Index", dt, RollFrequency::Quarterly, 4).unwrap();

        assert_eq!(candidates.len(), 4);
        // Should be Mar, Jun, Sep, Dec
        assert_eq!(candidates[0].month.month(), 3);
        assert_eq!(candidates[1].month.month(), 6);
        assert_eq!(candidates[2].month.month(), 9);
        assert_eq!(candidates[3].month.month(), 12);
    }

    #[test]
    fn test_reject_specific_ticker() {
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let result = generate_futures_candidates("ESH24 Index", dt, RollFrequency::Monthly, 3);
        assert!(result.is_err());
    }

    #[test]
    fn test_contract_index() {
        assert_eq!(contract_index("ES1 Index").unwrap(), 0);
        assert_eq!(contract_index("ES2 Index").unwrap(), 1);
        assert_eq!(contract_index("CL3 Comdty").unwrap(), 2);
    }

    #[test]
    fn test_validate_generic_ticker() {
        assert!(validate_generic_ticker("ES1 Index").is_ok());
        assert!(validate_generic_ticker("ESH24 Index").is_err());
    }

    #[test]
    fn test_filter_valid_contracts() {
        let contracts = vec![
            ("ESH24 Index".to_string(), "2024-03-15".to_string()),
            ("ESM24 Index".to_string(), "2024-06-21".to_string()),
            ("ESZ23 Index".to_string(), "2023-12-15".to_string()),
        ];
        let ref_date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let valid = filter_valid_contracts(&contracts, ref_date);

        assert_eq!(valid.len(), 2);
        assert_eq!(valid[0], "ESH24 Index");
        assert_eq!(valid[1], "ESM24 Index");
    }

    #[test]
    fn test_filter_valid_contracts_empty() {
        let contracts: Vec<(String, String)> = vec![];
        let ref_date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        assert!(filter_valid_contracts(&contracts, ref_date).is_empty());
    }

    #[test]
    fn test_filter_valid_contracts_invalid_dates() {
        let contracts = vec![
            ("ESH24 Index".to_string(), "not-a-date".to_string()),
            ("ESM24 Index".to_string(), "2024-06-21".to_string()),
        ];
        let ref_date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let valid = filter_valid_contracts(&contracts, ref_date);

        assert_eq!(valid.len(), 1);
        assert_eq!(valid[0], "ESM24 Index");
    }

    #[test]
    fn test_filter_candidates_by_cycle_quarterly() {
        let candidates = vec![
            ("ESF24 Index".to_string(), 2024, 1),
            ("ESG24 Index".to_string(), 2024, 2),
            ("ESH24 Index".to_string(), 2024, 3),
            ("ESJ24 Index".to_string(), 2024, 4),
            ("ESK24 Index".to_string(), 2024, 5),
            ("ESM24 Index".to_string(), 2024, 6),
        ];
        let filtered = filter_candidates_by_cycle(&candidates, "HMUZ");
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].0, "ESH24 Index");
        assert_eq!(filtered[1].0, "ESM24 Index");
    }

    #[test]
    fn test_filter_candidates_by_cycle_grains() {
        let candidates = vec![
            ("ZSF24 Comdty".to_string(), 2024, 1),
            ("ZSH24 Comdty".to_string(), 2024, 3),
            ("ZSK24 Comdty".to_string(), 2024, 5),
            ("ZSN24 Comdty".to_string(), 2024, 7),
            ("ZSQ24 Comdty".to_string(), 2024, 8),
            ("ZSU24 Comdty".to_string(), 2024, 9),
            ("ZSX24 Comdty".to_string(), 2024, 11),
        ];
        let filtered = filter_candidates_by_cycle(&candidates, "FHKNQUX");
        assert_eq!(filtered.len(), 7); // all match the soybean cycle
    }

    #[test]
    fn test_filter_candidates_by_cycle_empty() {
        let candidates: Vec<(String, i32, u32)> = vec![];
        assert!(filter_candidates_by_cycle(&candidates, "HMUZ").is_empty());
    }

    #[test]
    fn test_filter_candidates_by_cycle_case_insensitive() {
        let candidates = vec![("ESH24 Index".to_string(), 2024, 3)];
        let filtered = filter_candidates_by_cycle(&candidates, "hmuz");
        assert_eq!(filtered.len(), 1);
    }
}
