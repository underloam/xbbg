//! BQL (Bloomberg Query Language) query builders.
//!
//! Provides functions to construct BQL query strings for common workflows.

/// Build a BQL query for preferred stocks.
///
/// Uses Bloomberg's debt filter to find preferred stock issues
/// associated with a given equity ticker.
///
/// # Arguments
///
/// * `equity_ticker` - Company equity ticker (e.g., "BAC US Equity").
///   If no suffix is provided, " US Equity" will be appended.
/// * `fields` - Optional additional fields to retrieve.
///   Default fields are: id, name.
///
/// # Returns
///
/// A complete BQL query string.
///
/// # Examples
///
/// ```
/// use xbbg_ext::transforms::bql::build_preferreds_query;
///
/// let query = build_preferreds_query("BAC US Equity", &[]);
/// assert!(query.contains("debt("));
/// assert!(query.contains("Preferreds"));
///
/// let query2 = build_preferreds_query("BAC", &["px_last", "dvd_yld"]);
/// assert!(query2.contains("BAC US Equity"));
/// assert!(query2.contains("px_last"));
/// ```
pub fn build_preferreds_query(equity_ticker: &str, extra_fields: &[&str]) -> String {
    // Normalize ticker
    let ticker = if equity_ticker.contains(' ') {
        equity_ticker.to_string()
    } else {
        format!("{} US Equity", equity_ticker)
    };

    // Build field list
    let mut all_fields: Vec<&str> = vec!["id", "name"];
    for f in extra_fields {
        let lower = f.to_lowercase();
        if !all_fields.iter().any(|af| af.to_lowercase() == lower) {
            all_fields.push(f);
        }
    }

    let fields_str = all_fields.join(", ");

    format!(
        "get({}) for(filter(debt(['{}'], CONSOLIDATEDUPLICATES='N'), SRCH_ASSET_CLASS=='Preferreds'))",
        fields_str, ticker
    )
}

/// Build a BQL query for corporate bonds.
///
/// Uses Bloomberg's `debt()` universe to find corporate bond issues
/// for a given company via its equity ticker. This works across all
/// markets (US, JP, EU, etc.) because Bloomberg resolves the company
/// from the equity ticker rather than matching a raw TICKER field.
///
/// # Arguments
///
/// * `ticker` - Company equity ticker (e.g., "AAPL", "9984 JT Equity").
///   If no suffix is provided, " US Equity" is appended.
/// * `ccy` - Currency filter (None for all currencies).
/// * `extra_fields` - Optional additional fields to retrieve.
///   Default field is: id.
///
/// # Returns
///
/// A complete BQL query string.
///
/// # Examples
///
/// ```
/// use xbbg_ext::transforms::bql::build_corporate_bonds_query;
///
/// let query = build_corporate_bonds_query("AAPL", Some("USD"), &[]);
/// assert!(query.contains("debt("));
/// assert!(query.contains("AAPL US Equity"));
/// assert!(query.contains("Corporates"));
/// assert!(query.contains("CRNCY=='USD'"));
///
/// let query2 = build_corporate_bonds_query("9984 JT Equity", None, &["name", "cpn"]);
/// assert!(query2.contains("9984 JT Equity"));
/// assert!(!query2.contains("CRNCY"));
/// assert!(query2.contains("name"));
/// ```
pub fn build_corporate_bonds_query(
    ticker: &str,
    ccy: Option<&str>,
    extra_fields: &[&str],
) -> String {
    // Normalize ticker — append " US Equity" if no suffix provided
    let equity_ticker = if ticker.contains(' ') {
        ticker.to_string()
    } else {
        format!("{} US Equity", ticker)
    };

    // Build field list
    let mut all_fields: Vec<&str> = vec!["id"];
    for f in extra_fields {
        let lower = f.to_lowercase();
        if !all_fields.iter().any(|af| af.to_lowercase() == lower) {
            all_fields.push(f);
        }
    }

    let fields_str = all_fields.join(", ");

    // Build filter conditions
    let mut conditions = vec!["SRCH_ASSET_CLASS=='Corporates'".to_string()];

    if let Some(c) = ccy {
        conditions.push(format!("CRNCY=='{}'", c));
    }

    let filter_str = conditions.join(" AND ");

    format!(
        "get({}) for(filter(debt(['{}'], CONSOLIDATEDUPLICATES='N'), {}))",
        fields_str, equity_ticker, filter_str
    )
}

/// Build a BQL query for ETF holdings.
///
/// # Arguments
///
/// * `etf_ticker` - ETF ticker (e.g., "SPY US Equity" or "SPY").
///   If no suffix is provided, " US Equity" will be appended.
/// * `extra_fields` - Optional additional fields beyond the defaults
///   (id_isin, weights, id().position).
///
/// # Returns
///
/// A complete BQL query string.
///
/// # Examples
///
/// ```
/// use xbbg_ext::transforms::bql::build_etf_holdings_query;
///
/// let query = build_etf_holdings_query("SPY US Equity", &[]);
/// assert!(query.contains("holdings('SPY US Equity')"));
/// assert!(query.contains("id_isin"));
///
/// let query2 = build_etf_holdings_query("SPY", &["name", "px_last"]);
/// assert!(query2.contains("SPY US Equity"));
/// assert!(query2.contains("name"));
/// ```
pub fn build_etf_holdings_query(etf_ticker: &str, extra_fields: &[&str]) -> String {
    // Normalize ticker
    let ticker = if etf_ticker.contains(' ') {
        etf_ticker.to_string()
    } else {
        format!("{} US Equity", etf_ticker)
    };

    // Default fields
    let mut all_fields: Vec<&str> = vec!["id_isin", "weights", "id().position"];
    for f in extra_fields {
        if !all_fields.contains(f) {
            all_fields.push(f);
        }
    }

    let fields_str = all_fields.join(", ");

    format!("get({}) for(holdings('{}'))", fields_str, ticker)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_preferreds_query_basic() {
        let query = build_preferreds_query("BAC US Equity", &[]);
        assert!(query.contains("get(id, name)"));
        assert!(query.contains("debt(['BAC US Equity']"));
        assert!(query.contains("Preferreds"));
    }

    #[test]
    fn test_build_preferreds_query_auto_suffix() {
        let query = build_preferreds_query("BAC", &[]);
        assert!(query.contains("BAC US Equity"));
    }

    #[test]
    fn test_build_preferreds_query_extra_fields() {
        let query = build_preferreds_query("BAC US Equity", &["px_last", "dvd_yld"]);
        assert!(query.contains("px_last"));
        assert!(query.contains("dvd_yld"));
    }

    #[test]
    fn test_build_preferreds_query_no_dup_fields() {
        let query = build_preferreds_query("BAC", &["id", "name", "px_last"]);
        // "id" and "name" already in defaults, should not duplicate
        // "id" appears once in the fields and once in the filter, so check fields portion
        let fields_part = query.split("for(").next().unwrap();
        assert_eq!(
            fields_part.matches(", id").count()
                + if fields_part.starts_with("get(id") {
                    1
                } else {
                    0
                },
            1
        );
        assert!(query.contains("px_last"));
    }

    #[test]
    fn test_build_corporate_bonds_query_basic() {
        let query = build_corporate_bonds_query("AAPL", Some("USD"), &[]);
        assert!(query.contains("debt("));
        assert!(query.contains("AAPL US Equity"));
        assert!(query.contains("Corporates"));
        assert!(query.contains("CRNCY=='USD'"));
    }

    #[test]
    fn test_build_corporate_bonds_query_no_ccy() {
        let query = build_corporate_bonds_query("AAPL", None, &[]);
        assert!(!query.contains("CRNCY"));
    }

    #[test]
    fn test_build_corporate_bonds_query_intl_ticker() {
        let query = build_corporate_bonds_query("9984 JT Equity", None, &[]);
        assert!(query.contains("debt(['9984 JT Equity']"));
        assert!(!query.contains("TICKER"));
    }

    #[test]
    fn test_build_corporate_bonds_query_with_extra_fields() {
        let query = build_corporate_bonds_query("MSFT", Some("EUR"), &["name"]);
        assert!(query.contains("debt("));
        assert!(query.contains("MSFT US Equity"));
        assert!(query.contains("CRNCY=='EUR'"));
        assert!(query.contains("name"));
    }

    #[test]
    fn test_build_etf_holdings_query_basic() {
        let query = build_etf_holdings_query("SPY US Equity", &[]);
        assert!(query.contains("id_isin"));
        assert!(query.contains("weights"));
        assert!(query.contains("id().position"));
        assert!(query.contains("holdings('SPY US Equity')"));
    }

    #[test]
    fn test_build_etf_holdings_query_auto_suffix() {
        let query = build_etf_holdings_query("SPY", &[]);
        assert!(query.contains("SPY US Equity"));
    }

    #[test]
    fn test_build_etf_holdings_query_extra_fields() {
        let query = build_etf_holdings_query("SPY US Equity", &["name", "px_last"]);
        assert!(query.contains("name"));
        assert!(query.contains("px_last"));
    }
}
