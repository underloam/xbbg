//! Ticker parsing and normalization utilities.

use crate::constants::VALID_MONTH_CODES;
use crate::error::{ExtError, Result};

/// Parsed components of a Bloomberg ticker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickerParts {
    /// The prefix (e.g., "ES" for "ES1 Index")
    pub prefix: String,
    /// The index/position (e.g., 1 for "ES1 Index")
    pub index: u32,
    /// The asset class (e.g., "Index", "Equity", "Comdty", "Curncy")
    pub asset: String,
    /// Optional exchange/region for equity (e.g., "US" for "AAPL US Equity")
    pub exchange: Option<String>,
}

/// Parse a Bloomberg ticker into its component parts.
///
/// Handles various ticker formats:
/// - Futures: "ES1 Index", "CL1 Comdty"
/// - Equity futures: "SPY1 US Equity"
/// - CDX: "CDX IG CDSI GEN 5Y Corp"
///
/// # Examples
///
/// ```
/// use xbbg_ext::utils::ticker::parse_ticker_parts;
///
/// let parts = parse_ticker_parts("ES1 Index").unwrap();
/// assert_eq!(parts.prefix, "ES");
/// assert_eq!(parts.index, 1);
/// assert_eq!(parts.asset, "Index");
/// ```
///
/// # Errors
///
/// Returns `ExtError::InvalidTicker` if the ticker format is not recognized.
pub fn parse_ticker_parts(ticker: &str) -> Result<TickerParts> {
    let parts: Vec<&str> = ticker.split_whitespace().collect();

    if parts.is_empty() {
        return Err(ExtError::InvalidTicker(ticker.to_string()));
    }

    let asset = parts
        .last()
        .copied()
        .ok_or_else(|| ExtError::InvalidTicker(ticker.to_string()))?;

    match asset {
        "Index" | "Curncy" | "Comdty" | "Corp" => {
            // Format: PREFIX1 Asset (e.g., "ES1 Index")
            if parts.len() < 2 {
                return Err(ExtError::InvalidTicker(ticker.to_string()));
            }

            let base = parts[..parts.len() - 1].join(" ");
            let (prefix, index) = parse_prefix_index(&base)?;

            Ok(TickerParts {
                prefix,
                index,
                asset: asset.to_string(),
                exchange: None,
            })
        }
        "Equity" => {
            // Format: PREFIX1 EXCHANGE Equity (e.g., "SPY1 US Equity")
            if parts.len() < 3 {
                return Err(ExtError::InvalidTicker(ticker.to_string()));
            }

            let base = parts[0];
            let exchange = parts[1..parts.len() - 1].join(" ");
            let (prefix, index) = parse_prefix_index(base)?;

            Ok(TickerParts {
                prefix,
                index,
                asset: asset.to_string(),
                exchange: Some(exchange),
            })
        }
        _ => Err(ExtError::InvalidTicker(ticker.to_string())),
    }
}

/// Parse prefix and index from a base ticker string (e.g., "ES1" -> ("ES", 1)).
fn parse_prefix_index(base: &str) -> Result<(String, u32)> {
    // Find the last digit
    let base_bytes = base.as_bytes();
    let mut digit_pos = None;

    for (i, &b) in base_bytes.iter().enumerate().rev() {
        if b.is_ascii_digit() {
            digit_pos = Some(i);
            break;
        }
    }

    let digit_pos = digit_pos.ok_or_else(|| ExtError::InvalidTicker(base.to_string()))?;

    let prefix = &base[..digit_pos];
    let index_str = &base[digit_pos..digit_pos + 1];
    let index: u32 = index_str
        .parse()
        .map_err(|_| ExtError::InvalidTicker(base.to_string()))?;

    if prefix.is_empty() {
        return Err(ExtError::InvalidTicker(base.to_string()));
    }

    Ok((prefix.to_string(), index))
}

/// Check if a ticker appears to be a specific contract rather than generic.
///
/// Generic: "ES1 Index" (has digit 1-9)
/// Specific: "ESH24 Index" (has month code + year)
///
/// # Examples
///
/// ```
/// use xbbg_ext::utils::ticker::is_specific_contract;
///
/// assert!(!is_specific_contract("ES1 Index"));
/// assert!(is_specific_contract("ESH24 Index"));
/// assert!(is_specific_contract("ESH4 Index"));
/// ```
pub fn is_specific_contract(ticker: &str) -> bool {
    let parts: Vec<&str> = ticker.split_whitespace().collect();
    if parts.is_empty() {
        return false;
    }
    let base = parts[0];
    let bytes = base.as_bytes();
    let len = bytes.len();

    // Scan from the end: find trailing 1-2 year digits, then check for month code.
    // Pattern: [prefix][month_code][year_digits] where prefix is the root symbol.
    //
    // For 1-digit year: require prefix (root) length >= 2 to avoid false positives
    // on generic tickers like "UX1" (prefix="U", would be misread as month=X, year=1).
    // For 2-digit year: require prefix (root) length >= 1.
    //
    // Examples:
    //   "ESH24" -> prefix="ES", month='H', year="24" -> specific
    //   "UXZ5"  -> prefix="UX", month='Z', year="5"  -> specific
    //   "UX1"   -> prefix="U",  month='X', year="1"  -> prefix too short -> generic
    //   "ES1"   -> no valid month code before '1'       -> generic

    // Count trailing digits
    let mut digit_count = 0;
    for &b in bytes.iter().rev() {
        if b.is_ascii_digit() {
            digit_count += 1;
        } else {
            break;
        }
    }

    // All-digit token (e.g. "1" from "1 Index"): nothing before the digits
    // can be a month code, and `len - digit_count - 1` below would underflow.
    if len <= digit_count {
        return false;
    }

    // Need exactly 1 or 2 trailing digits
    if digit_count == 0 || digit_count > 2 {
        return false;
    }

    // Check if the character immediately before the trailing digits is a valid month code
    let month_pos = len - digit_count - 1;
    if month_pos == 0 {
        // Month code would be the first character -- no prefix, can't be specific
        return false;
    }

    let month_char = bytes[month_pos] as char;
    if !VALID_MONTH_CODES.contains(&month_char) {
        return false;
    }

    // Prefix is everything before the month code
    let prefix_len = month_pos;

    // For 1-digit year, require root prefix of at least 2 chars to avoid
    // misclassifying generic tickers like "UX1" (root=UX, index=1).
    // For 2-digit year, root prefix of 1+ is sufficient (e.g., "WH24").
    if digit_count == 1 && prefix_len < 2 {
        return false;
    }

    true
}

/// Normalize tickers to a list.
///
/// Takes ownership and returns the list unchanged: order, duplicates and
/// surrounding whitespace are preserved.
///
/// # Examples
///
/// ```
/// use xbbg_ext::utils::ticker::normalize_tickers;
///
/// let single = normalize_tickers(vec!["AAPL US Equity".to_string()]);
/// assert_eq!(single, vec!["AAPL US Equity"]);
///
/// let multi = normalize_tickers(vec![
///     "AAPL US Equity".to_string(),
///     "MSFT US Equity".to_string(),
/// ]);
/// assert_eq!(multi.len(), 2);
/// ```
pub fn normalize_tickers(tickers: Vec<String>) -> Vec<String> {
    tickers
}

/// Build a futures ticker from components.
///
/// # Arguments
///
/// * `prefix` - Ticker prefix (e.g., "ES")
/// * `month_code` - Month code (e.g., "H" for March)
/// * `year` - Year (can be 1 or 2 digits, e.g., "4" or "24")
/// * `asset` - Asset class (e.g., "Index")
///
/// # Examples
///
/// ```
/// use xbbg_ext::utils::ticker::build_futures_ticker;
///
/// let ticker = build_futures_ticker("ES", "H", "24", "Index");
/// assert_eq!(ticker, "ESH24 Index");
/// ```
pub fn build_futures_ticker(prefix: &str, month_code: &str, year: &str, asset: &str) -> String {
    format!("{}{}{} {}", prefix, month_code, year, asset)
}

/// Filter tickers to only equity tickers, in place.
///
/// Keeps tickers containing "Equity" and excludes equity options (containing "=").
/// Survivors keep their input order and are not copied.
pub fn filter_equity_tickers(mut tickers: Vec<String>) -> Vec<String> {
    tickers.retain(|t| t.contains("Equity") && !t.contains('='));
    tickers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_index_ticker() {
        let parts = parse_ticker_parts("ES1 Index").unwrap();
        assert_eq!(parts.prefix, "ES");
        assert_eq!(parts.index, 1);
        assert_eq!(parts.asset, "Index");
        assert_eq!(parts.exchange, None);
    }

    #[test]
    fn test_parse_comdty_ticker() {
        let parts = parse_ticker_parts("CL1 Comdty").unwrap();
        assert_eq!(parts.prefix, "CL");
        assert_eq!(parts.index, 1);
        assert_eq!(parts.asset, "Comdty");
    }

    #[test]
    fn test_parse_equity_ticker() {
        let parts = parse_ticker_parts("SPY1 US Equity").unwrap();
        assert_eq!(parts.prefix, "SPY");
        assert_eq!(parts.index, 1);
        assert_eq!(parts.asset, "Equity");
        assert_eq!(parts.exchange, Some("US".to_string()));
    }

    #[test]
    fn test_parse_invalid_ticker() {
        assert!(parse_ticker_parts("").is_err());
        assert!(parse_ticker_parts("INVALID").is_err());
        assert!(parse_ticker_parts("ES Index").is_err()); // No index
    }

    #[test]
    fn test_is_specific_contract() {
        // Generic tickers
        assert!(!is_specific_contract("ES1 Index"));
        assert!(!is_specific_contract("CL1 Comdty"));
        assert!(!is_specific_contract("SPY1 US Equity"));
        assert!(!is_specific_contract("UX1 Index")); // VIX generic 1st

        // Specific tickers
        assert!(is_specific_contract("ESH24 Index"));
        assert!(is_specific_contract("ESH4 Index"));
        assert!(is_specific_contract("CLZ24 Comdty"));
    }

    #[test]
    fn test_build_futures_ticker() {
        assert_eq!(
            build_futures_ticker("ES", "H", "24", "Index"),
            "ESH24 Index"
        );
        assert_eq!(
            build_futures_ticker("CL", "Z", "4", "Comdty"),
            "CLZ4 Comdty"
        );
    }

    #[test]
    fn test_filter_equity_tickers() {
        let tickers = vec![
            "AAPL US Equity".to_string(),
            "ES1 Index".to_string(),
            "MSFT US Equity".to_string(),
            "AAPL=US 01/15/24 C150 Equity".to_string(), // Option, should be excluded
            "AAPL US Equity".to_string(),
        ];
        let filtered = filter_equity_tickers(tickers);
        assert_eq!(
            filtered,
            vec!["AAPL US Equity", "MSFT US Equity", "AAPL US Equity"]
        );
    }

    #[test]
    fn test_normalize_tickers_preserves_input() {
        let input = vec![
            " MSFT US Equity".to_string(),
            "AAPL US Equity ".to_string(),
            " MSFT US Equity".to_string(),
        ];
        let tickers = normalize_tickers(input.clone());
        assert_eq!(tickers, input);
    }
}
