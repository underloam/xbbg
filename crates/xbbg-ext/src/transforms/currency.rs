//! Currency conversion utilities.
//!
//! Provides helpers for FX pair construction and currency adjustment.

/// Information needed for FX conversion.
#[derive(Debug, Clone)]
pub struct FxConversionInfo {
    /// The FX pair ticker (e.g., "USDGBP Curncy")
    pub fx_pair: String,
    /// Multiplier for pence/cents conversion (100.0 for GBp, 1.0 otherwise)
    pub factor: f64,
    /// Source currency
    pub from_ccy: String,
    /// Target currency
    pub to_ccy: String,
}

/// Build an FX pair ticker for currency conversion.
///
/// Handles special cases like British pence (GBp) vs pounds (GBP).
///
/// # Arguments
///
/// * `from_ccy` - Source currency code (e.g., "GBP", "GBp", "EUR")
/// * `to_ccy` - Target currency code (e.g., "USD")
///
/// # Returns
///
/// `FxConversionInfo` with the FX pair ticker and conversion factor.
///
/// # Examples
///
/// ```
/// use xbbg_ext::transforms::currency::build_fx_pair;
///
/// let info = build_fx_pair("GBP", "USD");
/// assert_eq!(info.fx_pair, "USDGBP Curncy");
/// assert_eq!(info.factor, 1.0);
///
/// // British pence need factor of 100
/// let info_pence = build_fx_pair("GBp", "USD");
/// assert_eq!(info_pence.fx_pair, "USDGBP Curncy");
/// assert_eq!(info_pence.factor, 100.0);
/// ```
pub fn build_fx_pair(from_ccy: &str, to_ccy: &str) -> FxConversionInfo {
    // Check for pence/cents (lowercase last char)
    let factor = if from_ccy
        .chars()
        .next_back()
        .is_some_and(|c| c.is_lowercase())
    {
        100.0
    } else {
        1.0
    };

    let from_upper = from_ccy.to_uppercase();
    let to_upper = to_ccy.to_uppercase();

    // FX pair format: TARGET_SOURCE Curncy
    let fx_pair = format!("{}{} Curncy", to_upper, from_upper);

    FxConversionInfo {
        fx_pair,
        factor,
        from_ccy: from_upper,
        to_ccy: to_upper,
    }
}

/// Check if two currencies are effectively the same.
///
/// Handles case-insensitive comparison and pence/cents equivalence.
///
/// # Examples
///
/// ```
/// use xbbg_ext::transforms::currency::same_currency;
///
/// assert!(same_currency("USD", "USD"));
/// assert!(same_currency("USD", "usd"));
/// assert!(same_currency("GBP", "GBp"));  // GBP and pence
/// assert!(!same_currency("USD", "EUR"));
/// ```
pub fn same_currency(ccy1: &str, ccy2: &str) -> bool {
    ccy1.to_uppercase() == ccy2.to_uppercase()
}

/// Extract unique currencies from a list that need FX conversion.
///
/// Returns the upper-cased currencies that differ from the target currency,
/// sorted and deduplicated. Accepts `&str` or `String` items.
///
/// # Examples
///
/// ```
/// use xbbg_ext::transforms::currency::currencies_needing_conversion;
///
/// let currencies = vec!["USD", "GBP", "EUR", "USD"];
/// let need_fx = currencies_needing_conversion(&currencies, "USD");
/// assert_eq!(need_fx, vec!["EUR", "GBP"]);
/// ```
pub fn currencies_needing_conversion<S: AsRef<str>>(currencies: &[S], target: &str) -> Vec<String> {
    let target_upper = target.to_uppercase();
    let mut unique: Vec<String> = currencies
        .iter()
        .map(|c| AsRef::<str>::as_ref(c).to_uppercase())
        .filter(|c| *c != target_upper)
        .collect();

    unique.sort();
    unique.dedup();
    unique
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_fx_pair_standard() {
        let info = build_fx_pair("EUR", "USD");
        assert_eq!(info.fx_pair, "USDEUR Curncy");
        assert_eq!(info.factor, 1.0);
        assert_eq!(info.from_ccy, "EUR");
        assert_eq!(info.to_ccy, "USD");
    }

    #[test]
    fn test_build_fx_pair_pence() {
        let info = build_fx_pair("GBp", "USD");
        assert_eq!(info.fx_pair, "USDGBP Curncy");
        assert_eq!(info.factor, 100.0);
    }

    #[test]
    fn test_same_currency() {
        assert!(same_currency("USD", "USD"));
        assert!(same_currency("USD", "usd"));
        assert!(same_currency("GBP", "GBp"));
        assert!(!same_currency("USD", "EUR"));
    }

    #[test]
    fn test_currencies_needing_conversion() {
        let currencies = vec!["USD", "GBP", "eur", "USD", "GBP"];
        let need_fx = currencies_needing_conversion(&currencies, "usd");
        assert_eq!(need_fx, vec!["EUR", "GBP"]);

        let owned: Vec<String> = currencies.iter().map(|c| c.to_string()).collect();
        assert_eq!(currencies_needing_conversion(&owned, "USD"), need_fx);
    }

    #[test]
    fn test_currencies_needing_conversion_none() {
        let currencies = vec!["USD", "USD"];
        let need_fx = currencies_needing_conversion(&currencies, "USD");
        assert!(need_fx.is_empty());
    }
}
