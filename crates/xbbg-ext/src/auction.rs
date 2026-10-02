//! Stream-valid auction fields and pure primary-venue routing decisions.
//!
//! A route is only a candidate until [`validate_venue`] checks the reference
//! fields returned for its topic. In particular, equity ISIN PCS suffixes must
//! not be used to route equities: Bloomberg can silently return the composite.

use std::collections::HashMap;
use std::sync::LazyLock;

use serde::Deserialize;

pub const IMBALANCE: &[&str] = &[
    "ORDER_IMB_BUY_VOLUME",
    "ORDER_IMB_SELL_VOLUME",
    "IMBALANCE_INDIC_RT",
    "IMBALANCE_TIMESTAMP_RT",
    "PAIRED_VOLUME_AT_REFERENCE_PX_RT",
    "REFERENCE_PRICE_RT",
    "MARKET_ORDER_IMBALANCE_VOLUME_RT",
    "MARKET_ORDER_IMBALANCE_SIDE_RT",
    "NEAR_REFERENCE_PX_VARIATION_RT",
    "IMBALANCE_CROSS_TYPE_RT",
];
pub const INDICATIVE: &[&str] = &[
    "INDICATIVE_NEAR",
    "INDICATIVE_FAR",
    "THEO_PRICE",
    "VOLUME_THEO",
    "THEORETICAL_TIME_TODAY_RT",
    "IMBALANCE_BUY",
    "IMBALANCE_SELL",
];
pub const STATE: &[&str] = &[
    "AUCTION_TYPE_REALTIME",
    "IN_AUCTION_RT",
    "TIME_AUCTION_CALL_CONCLUSION_RT",
    "SUB_SEC_TM_AUCT_CALL_CNCLSN_RT",
    "AUCTION_EXTENSION_RT",
    "MARKET_OR_LIMIT_CLOSE_ENTRY_RT",
    "MOC_ELIGIBLE_RT",
    "RT_TRADING_PERIOD",
    "RT_EXCH_MARKET_STATUS",
    "RT_SIMP_SEC_STATUS",
];
pub const HALTS: &[&str] = &[
    "TRADING_HALT_REASON_TYPE_RT",
    "LULD_EVENT_CODE_RT",
    "INTRADAY_AUCTION_VOLUME_RT",
];
pub const RESULTS: &[&str] = &[
    "OFFICIAL_CLOSE_AUCTION_PRICE_RT",
    "OFFICIAL_CLOSE_AUCTION_VOLUME_RT",
    "CLOSING_AUCTION_VOLUME_RT",
    "CLOSING_AUCTION_VOLUME_DATE_RT",
    "NUM_TRADES_CLOSING_AUCTION_RT",
    "OFFICIAL_OPEN_AUCTION_PRICE_RT",
    "OFFICIAL_OPEN_AUCTION_VOLUME_RT",
    "OPENING_AUCTION_VOLUME_RT",
    "PX_OFFICIAL_CLOSE_RT",
];
pub const COMPOSITE: &[&str] = &[
    "PRIMARY_MARKET_CLOSING_PRICE_RT",
    "TIME_OF_CLOS_PX_ON_PRIM_MKT_RT",
    "PRIMARY_MARKET_OPENING_PRICE_RT",
];
pub const QUOTES: &[&str] = &["BID", "ASK", "BID_SIZE", "ASK_SIZE"];
pub const DEFAULT: &[&str] = &concat_groups::<
    { IMBALANCE.len() + INDICATIVE.len() + STATE.len() + HALTS.len() + RESULTS.len() },
>(&[IMBALANCE, INDICATIVE, STATE, HALTS, RESULTS]);

/// Prices for which Bloomberg uses zero to mean "no price".
///
/// This sentinel list is not a field group. Intersect it with the requested fields
/// when choosing which zero prices to materialise as null.
pub const ZERO_PRICE_FIELDS: &[&str] = &[
    "THEO_PRICE",
    "INDICATIVE_NEAR",
    "INDICATIVE_FAR",
    "IMBALANCE_BUY",
    "IMBALANCE_SELL",
    "REFERENCE_PRICE_RT",
];

const fn concat_groups<const N: usize>(groups: &[&[&'static str]]) -> [&'static str; N] {
    let mut fields = [""; N];
    let mut offset = 0;
    let mut group = 0;
    while group < groups.len() {
        let mut field = 0;
        while field < groups[group].len() {
            fields[offset] = groups[group][field];
            offset += 1;
            field += 1;
        }
        group += 1;
    }
    fields
}

/// Look up a stream-valid field group, ignoring case and surrounding whitespace.
pub fn field_group(name: &str) -> Option<&'static [&'static str]> {
    let name = name.trim();
    field_group_names()
        .iter()
        .position(|candidate| name.eq_ignore_ascii_case(candidate))
        .map(|index| {
            [
                IMBALANCE, INDICATIVE, STATE, HALTS, RESULTS, COMPOSITE, QUOTES, DEFAULT,
            ][index]
        })
}

/// Field-group lookup names, in their public display order.
pub fn field_group_names() -> &'static [&'static str] {
    &[
        "imbalance",
        "indicative",
        "state",
        "halts",
        "results",
        "composite",
        "quotes",
        "default",
    ]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImbalanceSide {
    Buy,
    Sell,
    NoImbalance,
}

impl ImbalanceSide {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
            Self::NoImbalance => "none",
        }
    }
}

/// Interpret directional codes without treating unavailable/undisclosed data as zero.
pub fn imbalance_side(code: &str) -> Option<ImbalanceSide> {
    let code = code.trim();
    if ["BUY", "MBUY", "RBUY"]
        .iter()
        .any(|value| code.eq_ignore_ascii_case(value))
    {
        Some(ImbalanceSide::Buy)
    } else if ["SELL", "MSEL", "RSEL"]
        .iter()
        .any(|value| code.eq_ignore_ascii_case(value))
    {
        Some(ImbalanceSide::Sell)
    } else if ["NOIM", "NIMB"]
        .iter()
        .any(|value| code.eq_ignore_ascii_case(value))
    {
        Some(ImbalanceSide::NoImbalance)
    } else {
        None
    }
}

#[derive(Deserialize)]
struct PricingSources {
    pfd_exchange_pcs: HashMap<String, String>,
}

static PRICING_SOURCES: LazyLock<PricingSources> = LazyLock::new(|| {
    let sources: PricingSources = toml::from_str(include_str!("../data/pricing_sources.toml"))
        .expect("embedded preferred pricing-source table must be valid TOML");
    PricingSources {
        pfd_exchange_pcs: sources
            .pfd_exchange_pcs
            .into_iter()
            .map(|(name, pcs)| (exchange_key(&name), pcs))
            .collect(),
    }
});

fn exchange_key(name: &str) -> String {
    let mut key = String::with_capacity(name.len());
    for word in name.split_whitespace() {
        if !key.is_empty() {
            key.push(' ');
        }
        key.extend(word.chars().flat_map(char::to_uppercase));
    }
    key
}

/// Resolve a preferred exchange name to a PCS; normalised per-call overrides win.
pub fn pfd_pricing_source(
    exch_code_name: &str,
    overrides: &HashMap<String, String>,
) -> Option<String> {
    let key = exchange_key(exch_code_name);
    let overridden = overrides.get(&key).or_else(|| {
        overrides
            .iter()
            .find_map(|(name, pcs)| (exchange_key(name) == key).then_some(pcs))
    });
    if let Some(pcs) = overridden {
        return text(Some(pcs)).map(str::to_ascii_uppercase);
    }
    PRICING_SOURCES.pfd_exchange_pcs.get(&key).cloned()
}

/// Validate the uppercase ISO 6166 syntax and expanded-alphanumeric Luhn check digit.
pub fn is_valid_isin(isin: &str) -> bool {
    let bytes = isin.as_bytes();
    if bytes.len() != 12
        || !bytes[..2].iter().all(u8::is_ascii_uppercase)
        || !bytes[2..11]
            .iter()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        || !bytes[11].is_ascii_digit()
    {
        return false;
    }

    let mut sum = 0u32;
    let mut double = false;
    let mut add_digit = |digit: u8| {
        let value = u32::from(digit) * if double { 2 } else { 1 };
        sum += value / 10 + value % 10;
        double = !double;
    };
    for &byte in bytes.iter().rev() {
        if byte.is_ascii_digit() {
            add_digit(byte - b'0');
        } else {
            let value = byte - b'A' + 10;
            add_digit(value % 10);
            add_digit(value / 10);
        }
    }
    sum.is_multiple_of(10)
}

/// Trim an input and put bare, valid ISINs into Bloomberg's lookup namespace.
pub fn normalize_security_input(security: &str) -> String {
    let security = security.trim();
    if is_valid_isin(security) {
        format!("/isin/{security}")
    } else {
        security.to_string()
    }
}

pub fn equity_venue_ticker(ticker: &str, exch_code: &str) -> String {
    format!("{} {} Equity", ticker.trim(), exch_code.trim())
}

pub fn pcs_isin_topic(isin: &str, pcs: &str) -> String {
    format!("/isin/{}@{}", isin.trim(), pcs.trim())
}

/// Reference fields for one normalised lookup, or for its validation response.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VenueReference<'a> {
    pub market_sector_des: Option<&'a str>,
    pub ticker: Option<&'a str>,
    pub exch_code: Option<&'a str>,
    pub composite_exch_code: Option<&'a str>,
    pub eqy_prim_exch_shrt: Option<&'a str>,
    pub id_mic_prim_exch: Option<&'a str>,
    pub pricing_source: Option<&'a str>,
    pub id_isin: Option<&'a str>,
    pub parsekyable_des: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueMethod {
    ExchangeTicker,
    PcsSuffix,
    AsIs,
}

impl VenueMethod {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExchangeTicker => "exchange_ticker",
            Self::PcsSuffix => "pcs_suffix",
            Self::AsIs => "as_is",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueStatus {
    Resolved,
    Unresolved,
    Unsupported,
    Mismatch,
}

impl VenueStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Resolved => "resolved",
            Self::Unresolved => "unresolved",
            Self::Unsupported => "unsupported",
            Self::Mismatch => "mismatch",
        }
    }
}

/// A candidate topic and the independent reference-data check needed to trust it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueDecision {
    pub kind: String,
    pub composite: Option<String>,
    pub venue_topic: Option<String>,
    pub method: Option<VenueMethod>,
    pub expected_exch_code: Option<String>,
    pub expected_pcs: Option<String>,
    pub mic: Option<String>,
    pub status: VenueStatus,
    pub error: Option<String>,
}

/// Route an equity or preferred, respecting an already exchange-specific lookup.
///
/// Successful routing leaves `status` unresolved until [`validate_venue`] confirms
/// the returned exchange/PCS. Missing fields and unsupported sectors never route.
pub fn route_venue(
    lookup: &str,
    reference: &VenueReference<'_>,
    pcs_overrides: &HashMap<String, String>,
) -> VenueDecision {
    let sector = text(reference.market_sector_des);
    let mut decision = VenueDecision {
        kind: sector
            .map(str::to_ascii_lowercase)
            .unwrap_or_else(|| "unknown".to_string()),
        composite: None,
        venue_topic: None,
        method: None,
        expected_exch_code: None,
        expected_pcs: None,
        mic: text(reference.id_mic_prim_exch).map(str::to_string),
        status: VenueStatus::Unresolved,
        error: None,
    };
    let routed = (|| -> Result<(), String> {
        let sector = required(sector, "MARKET_SECTOR_DES")?;
        if sector.eq_ignore_ascii_case("Equity") {
            let ticker = required(reference.ticker, "TICKER")?;
            let composite = required(reference.composite_exch_code, "COMPOSITE_EXCH_CODE")?;
            decision.composite = Some(equity_venue_ticker(ticker, composite));
            let exchange = required(reference.exch_code, "EXCH_CODE")?;
            if exchange.eq_ignore_ascii_case(composite) {
                let primary = required(reference.eqy_prim_exch_shrt, "EQY_PRIM_EXCH_SHRT")?;
                decision.venue_topic = Some(equity_venue_ticker(ticker, primary));
                decision.method = Some(VenueMethod::ExchangeTicker);
                decision.expected_exch_code = Some(primary.to_string());
            } else {
                decision.venue_topic = Some(required(Some(lookup), "lookup")?.to_string());
                decision.method = Some(VenueMethod::AsIs);
                decision.expected_exch_code = Some(exchange.to_string());
            }
        } else if sector.eq_ignore_ascii_case("Pfd") {
            let source = required(reference.pricing_source, "PRICING_SOURCE")?;
            if source.eq_ignore_ascii_case("EXCH") {
                let exchange = required(reference.exch_code, "EXCH_CODE")?;
                let isin = required(reference.id_isin, "ID_ISIN")?;
                if !is_valid_isin(isin) {
                    return Err("ID_ISIN is not a valid ISO 6166 identifier".to_string());
                }
                let Some(pcs) = pfd_pricing_source(exchange, pcs_overrides) else {
                    decision.status = VenueStatus::Unsupported;
                    return Err(format!(
                        "no preferred pricing source for exchange '{exchange}'; pass pcs_overrides"
                    ));
                };
                decision.venue_topic = Some(pcs_isin_topic(isin, &pcs));
                decision.method = Some(VenueMethod::PcsSuffix);
                decision.expected_pcs = Some(pcs);
            } else {
                decision.venue_topic = Some(required(Some(lookup), "lookup")?.to_string());
                decision.method = Some(VenueMethod::AsIs);
                decision.expected_pcs = Some(source.to_string());
            }
        } else {
            decision.status = VenueStatus::Unsupported;
            return Err(format!(
                "auction venue routing does not support market sector '{sector}'"
            ));
        }
        Ok(())
    })();
    decision.error = Some(match routed {
        Ok(()) => "venue validation is required".to_string(),
        Err(error) => error,
    });
    decision
}

/// Validate the routed topic without accepting Bloomberg's silent fallback listing.
/// Missing validation fields remain unresolved; wrong exchange/PCS is a mismatch.
pub fn validate_venue(decision: &mut VenueDecision, reference: &VenueReference<'_>) {
    if decision.venue_topic.is_none() {
        return;
    }
    let (field, expected, actual) = if let Some(expected) = decision.expected_exch_code.as_deref() {
        ("EXCH_CODE", expected, text(reference.exch_code))
    } else if let Some(expected) = decision.expected_pcs.as_deref() {
        ("PRICING_SOURCE", expected, text(reference.pricing_source))
    } else {
        decision.status = VenueStatus::Unresolved;
        decision.error =
            Some("venue has no exchange or pricing-source validation target".to_string());
        return;
    };
    match actual {
        Some(actual) if actual.eq_ignore_ascii_case(expected) => {
            decision.status = VenueStatus::Resolved;
            decision.error = None;
        }
        Some(actual) => {
            decision.status = VenueStatus::Mismatch;
            decision.error = Some(format!(
                "venue {field} mismatch: expected '{expected}', got '{actual}'"
            ));
        }
        None => {
            decision.status = VenueStatus::Unresolved;
            decision.error = Some(format!("venue validation did not return {field}"));
        }
    }
}

fn text(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| {
        !value.is_empty()
            && !["N/A", "N.A.", "#N/A"]
                .iter()
                .any(|missing| value.eq_ignore_ascii_case(missing))
    })
}

fn required<'a>(value: Option<&'a str>, field: &str) -> Result<&'a str, String> {
    text(value).ok_or_else(|| format!("missing {field} for venue routing"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn synthetic_isin() -> String {
        // ZZ is deliberately synthetic; calculate the check digit independently.
        let body = "ZZ000000001";
        let mut digits = Vec::new();
        for byte in body.bytes() {
            if byte.is_ascii_digit() {
                digits.push(u32::from(byte - b'0'));
            } else {
                let value = u32::from(byte - b'A' + 10);
                digits.extend([value / 10, value % 10]);
            }
        }
        let sum: u32 = digits
            .iter()
            .rev()
            .enumerate()
            .map(|(index, digit)| {
                let value = digit * if index % 2 == 0 { 2 } else { 1 };
                value / 10 + value % 10
            })
            .sum();
        format!("{body}{}", (10 - sum % 10) % 10)
    }

    fn equity() -> VenueReference<'static> {
        VenueReference {
            market_sector_des: Some("Equity"),
            ticker: Some("IBM"),
            exch_code: Some("US"),
            composite_exch_code: Some("US"),
            eqy_prim_exch_shrt: Some("UN"),
            id_mic_prim_exch: Some("XNYS"),
            ..Default::default()
        }
    }

    #[test]
    fn isin_check_digit_and_security_normalisation() {
        let isin = synthetic_isin();
        assert!(is_valid_isin(&isin));
        assert_eq!(
            normalize_security_input(&format!(" {isin} ")),
            format!("/isin/{isin}")
        );
        for digit in b'0'..=b'9' {
            let candidate = format!("{}{}", &isin[..11], char::from(digit));
            assert_eq!(is_valid_isin(&candidate), candidate == isin);
        }
        for invalid in [
            "",
            "ZZ00000000",
            "ZZ000000001A",
            "0Z0000000010",
            "ZZ0000000!10",
            "éZ0000000010",
        ] {
            assert!(!is_valid_isin(invalid), "{invalid}");
        }
        let lower = isin.to_ascii_lowercase();
        assert!(!is_valid_isin(&lower));
        for security in [
            "IBM US Equity".to_string(),
            format!("/isin/{isin}"),
            "/bbgid/BBG000TEST001".to_string(),
            format!("/isin/{isin}@SNY2"),
            lower,
        ] {
            assert_eq!(normalize_security_input(&format!(" {security} ")), security);
        }
        assert_eq!(equity_venue_ticker(" IBM ", " UN "), "IBM UN Equity");
        assert_eq!(pcs_isin_topic(&isin, "SNY2"), format!("/isin/{isin}@SNY2"));
    }

    #[test]
    fn imbalance_codes_distinguish_none_from_unavailable() {
        for code in ["BUY", "mbuy", " RBUY "] {
            assert_eq!(imbalance_side(code).map(ImbalanceSide::as_str), Some("buy"));
        }
        for code in ["SELL", "msel", " RSEL "] {
            assert_eq!(
                imbalance_side(code).map(ImbalanceSide::as_str),
                Some("sell")
            );
        }
        for code in ["NOIM", " nimb "] {
            assert_eq!(
                imbalance_side(code).map(ImbalanceSide::as_str),
                Some("none")
            );
        }
        for code in ["INOR", "NODS", "", " ", "N.A.", "unknown"] {
            assert_eq!(imbalance_side(code), None);
        }
    }

    #[test]
    fn field_groups_keep_stream_names_and_default_order() {
        assert_eq!(
            field_group_names(),
            &[
                "imbalance",
                "indicative",
                "state",
                "halts",
                "results",
                "composite",
                "quotes",
                "default"
            ]
        );
        let expected: Vec<_> = [IMBALANCE, INDICATIVE, STATE, HALTS, RESULTS].concat();
        assert_eq!(DEFAULT, expected);
        assert_eq!(field_group(" DEFAULT "), Some(DEFAULT));
        assert_eq!(field_group("missing"), None);
        for name in field_group_names() {
            let fields = field_group(name).unwrap();
            assert_eq!(
                fields.iter().copied().collect::<HashSet<_>>().len(),
                fields.len()
            );
            assert!(!fields.contains(&"PX_THEO"));
            assert!(!fields.contains(&"PX_BID"));
        }
        assert_eq!(
            IMBALANCE,
            &[
                "ORDER_IMB_BUY_VOLUME",
                "ORDER_IMB_SELL_VOLUME",
                "IMBALANCE_INDIC_RT",
                "IMBALANCE_TIMESTAMP_RT",
                "PAIRED_VOLUME_AT_REFERENCE_PX_RT",
                "REFERENCE_PRICE_RT",
                "MARKET_ORDER_IMBALANCE_VOLUME_RT",
                "MARKET_ORDER_IMBALANCE_SIDE_RT",
                "NEAR_REFERENCE_PX_VARIATION_RT",
                "IMBALANCE_CROSS_TYPE_RT"
            ]
        );
        assert_eq!(
            INDICATIVE,
            &[
                "INDICATIVE_NEAR",
                "INDICATIVE_FAR",
                "THEO_PRICE",
                "VOLUME_THEO",
                "THEORETICAL_TIME_TODAY_RT",
                "IMBALANCE_BUY",
                "IMBALANCE_SELL"
            ]
        );
        assert_eq!(
            STATE,
            &[
                "AUCTION_TYPE_REALTIME",
                "IN_AUCTION_RT",
                "TIME_AUCTION_CALL_CONCLUSION_RT",
                "SUB_SEC_TM_AUCT_CALL_CNCLSN_RT",
                "AUCTION_EXTENSION_RT",
                "MARKET_OR_LIMIT_CLOSE_ENTRY_RT",
                "MOC_ELIGIBLE_RT",
                "RT_TRADING_PERIOD",
                "RT_EXCH_MARKET_STATUS",
                "RT_SIMP_SEC_STATUS"
            ]
        );
        assert_eq!(
            HALTS,
            &[
                "TRADING_HALT_REASON_TYPE_RT",
                "LULD_EVENT_CODE_RT",
                "INTRADAY_AUCTION_VOLUME_RT"
            ]
        );
        assert_eq!(
            RESULTS,
            &[
                "OFFICIAL_CLOSE_AUCTION_PRICE_RT",
                "OFFICIAL_CLOSE_AUCTION_VOLUME_RT",
                "CLOSING_AUCTION_VOLUME_RT",
                "CLOSING_AUCTION_VOLUME_DATE_RT",
                "NUM_TRADES_CLOSING_AUCTION_RT",
                "OFFICIAL_OPEN_AUCTION_PRICE_RT",
                "OFFICIAL_OPEN_AUCTION_VOLUME_RT",
                "OPENING_AUCTION_VOLUME_RT",
                "PX_OFFICIAL_CLOSE_RT"
            ]
        );
        assert_eq!(
            COMPOSITE,
            &[
                "PRIMARY_MARKET_CLOSING_PRICE_RT",
                "TIME_OF_CLOS_PX_ON_PRIM_MKT_RT",
                "PRIMARY_MARKET_OPENING_PRICE_RT"
            ]
        );
        assert_eq!(QUOTES, &["BID", "ASK", "BID_SIZE", "ASK_SIZE"]);
    }

    #[test]
    fn zero_price_sentinels_are_not_a_field_group() {
        assert_eq!(
            ZERO_PRICE_FIELDS,
            &[
                "THEO_PRICE",
                "INDICATIVE_NEAR",
                "INDICATIVE_FAR",
                "IMBALANCE_BUY",
                "IMBALANCE_SELL",
                "REFERENCE_PRICE_RT",
            ]
        );
        assert_eq!(field_group("zero_price_fields"), None);
        assert!(!field_group_names().contains(&"zero_price_fields"));
        assert!(!ZERO_PRICE_FIELDS.contains(&"ORDER_IMB_BUY_VOLUME"));
        assert!(!ZERO_PRICE_FIELDS.contains(&"OFFICIAL_CLOSE_AUCTION_PRICE_RT"));
    }

    #[test]
    fn pricing_sources_normalise_keys_and_prefer_overrides() {
        let overrides = HashMap::from([("  new\t york ".to_string(), " custom ".to_string())]);
        assert_eq!(
            pfd_pricing_source("NEW   YORK", &overrides).as_deref(),
            Some("CUSTOM")
        );
        for (exchange, source) in [
            (" new york ", "SNY2"),
            ("NASDAQ/NGS", "NASP"),
            ("NASDAQ/NGM", "NASP"),
            ("NASDAQ/NCM", "NASP"),
            ("NYSE AMERICAN", "AMEX"),
        ] {
            assert_eq!(
                pfd_pricing_source(exchange, &HashMap::new()).as_deref(),
                Some(source)
            );
        }
        assert_eq!(pfd_pricing_source("UNMAPPED", &HashMap::new()), None);
    }

    #[test]
    fn composite_equity_routes_to_primary_exchange_and_validates() {
        let mut decision = route_venue("IBM US Equity", &equity(), &HashMap::new());
        assert_eq!(decision.kind, "equity");
        assert_eq!(decision.composite.as_deref(), Some("IBM US Equity"));
        assert_eq!(decision.venue_topic.as_deref(), Some("IBM UN Equity"));
        assert_eq!(decision.method, Some(VenueMethod::ExchangeTicker));
        assert_eq!(decision.mic.as_deref(), Some("XNYS"));
        assert_eq!(decision.status, VenueStatus::Unresolved);
        validate_venue(
            &mut decision,
            &VenueReference {
                exch_code: Some("UN"),
                ..Default::default()
            },
        );
        assert_eq!(decision.status, VenueStatus::Resolved);
        assert_eq!(decision.error, None);
        validate_venue(
            &mut decision,
            &VenueReference {
                exch_code: Some("US"),
                ..Default::default()
            },
        );
        assert_eq!(decision.status, VenueStatus::Mismatch);
        assert!(decision.error.as_deref().unwrap().contains("EXCH_CODE"));
        validate_venue(&mut decision, &VenueReference::default());
        assert_eq!(decision.status, VenueStatus::Unresolved);
    }

    #[test]
    fn explicit_equity_venue_is_respected_instead_of_primary() {
        let reference = VenueReference {
            exch_code: Some("UW"),
            eqy_prim_exch_shrt: None,
            ..equity()
        };
        let decision = route_venue("/bbgid/BBG000TEST001", &reference, &HashMap::new());
        assert_eq!(decision.method, Some(VenueMethod::AsIs));
        assert_eq!(
            decision.venue_topic.as_deref(),
            Some("/bbgid/BBG000TEST001")
        );
        assert_eq!(decision.expected_exch_code.as_deref(), Some("UW"));
    }

    #[test]
    fn preferred_exchange_routes_through_table_or_override_and_validates() {
        let isin = synthetic_isin();
        let reference = VenueReference {
            market_sector_des: Some("Pfd"),
            pricing_source: Some("EXCH"),
            exch_code: Some("NEW YORK"),
            id_isin: Some(&isin),
            ..Default::default()
        };
        let mut decision = route_venue(&format!("/isin/{isin}"), &reference, &HashMap::new());
        assert_eq!(decision.kind, "pfd");
        assert_eq!(decision.composite, None);
        assert_eq!(decision.method, Some(VenueMethod::PcsSuffix));
        assert_eq!(decision.venue_topic, Some(format!("/isin/{isin}@SNY2")));
        validate_venue(
            &mut decision,
            &VenueReference {
                pricing_source: Some("SNY2"),
                ..Default::default()
            },
        );
        assert_eq!(decision.status, VenueStatus::Resolved);
        validate_venue(
            &mut decision,
            &VenueReference {
                pricing_source: Some("EXCH"),
                ..Default::default()
            },
        );
        assert_eq!(decision.status, VenueStatus::Mismatch);
        assert!(decision
            .error
            .as_deref()
            .unwrap()
            .contains("PRICING_SOURCE"));
        validate_venue(&mut decision, &VenueReference::default());
        assert_eq!(decision.status, VenueStatus::Unresolved);
        let overrides = HashMap::from([("new york".to_string(), "CUSTOM".to_string())]);
        let overridden = route_venue(&isin, &reference, &overrides);
        assert_eq!(overridden.venue_topic, Some(format!("/isin/{isin}@CUSTOM")));
    }

    #[test]
    fn unmapped_preferred_exchange_is_actionably_unsupported() {
        let isin = synthetic_isin();
        let reference = VenueReference {
            market_sector_des: Some("Pfd"),
            pricing_source: Some("EXCH"),
            exch_code: Some("SYNTHETIC EXCHANGE"),
            id_isin: Some(&isin),
            ..Default::default()
        };
        let decision = route_venue(&isin, &reference, &HashMap::new());
        assert_eq!(decision.status, VenueStatus::Unsupported);
        assert_eq!(decision.venue_topic, None);
        let error = decision.error.unwrap();
        assert!(error.contains("SYNTHETIC EXCHANGE"));
        assert!(error.contains("pcs_overrides"));
    }

    #[test]
    fn already_routed_preferred_stays_as_is() {
        let reference = VenueReference {
            market_sector_des: Some("Pfd"),
            pricing_source: Some("SNY2"),
            ..Default::default()
        };
        let decision = route_venue("SYNTHETIC@SNY2 Pfd", &reference, &HashMap::new());
        assert_eq!(decision.method, Some(VenueMethod::AsIs));
        assert_eq!(decision.venue_topic.as_deref(), Some("SYNTHETIC@SNY2 Pfd"));
        assert_eq!(decision.expected_pcs.as_deref(), Some("SNY2"));
    }

    #[test]
    fn missing_routing_fields_and_other_sectors_never_route() {
        for (reference, missing) in [
            (VenueReference::default(), "MARKET_SECTOR_DES"),
            (
                VenueReference {
                    ticker: None,
                    ..equity()
                },
                "TICKER",
            ),
            (
                VenueReference {
                    exch_code: None,
                    ..equity()
                },
                "EXCH_CODE",
            ),
            (
                VenueReference {
                    composite_exch_code: Some("N/A"),
                    ..equity()
                },
                "COMPOSITE_EXCH_CODE",
            ),
            (
                VenueReference {
                    eqy_prim_exch_shrt: Some(" "),
                    ..equity()
                },
                "EQY_PRIM_EXCH_SHRT",
            ),
            (
                VenueReference {
                    market_sector_des: Some("Pfd"),
                    ..Default::default()
                },
                "PRICING_SOURCE",
            ),
            (
                VenueReference {
                    market_sector_des: Some("Pfd"),
                    pricing_source: Some("EXCH"),
                    ..Default::default()
                },
                "EXCH_CODE",
            ),
            (
                VenueReference {
                    market_sector_des: Some("Pfd"),
                    pricing_source: Some("EXCH"),
                    exch_code: Some("NEW YORK"),
                    ..Default::default()
                },
                "ID_ISIN",
            ),
        ] {
            let decision = route_venue("SYNTHETIC", &reference, &HashMap::new());
            assert_eq!(decision.status, VenueStatus::Unresolved);
            assert_eq!(decision.venue_topic, None);
            assert!(decision.error.unwrap().contains(missing));
        }
        let decision = route_venue(
            "SYNTHETIC Corp",
            &VenueReference {
                market_sector_des: Some("Corp"),
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert_eq!(decision.kind, "corp");
        assert_eq!(decision.status, VenueStatus::Unsupported);
        assert_eq!(decision.venue_topic, None);
        assert!(decision.error.unwrap().contains("Corp"));
    }

    #[test]
    fn sentinel_shaped_tickers_are_literal_identifiers() {
        for ticker in ["nan", "null"] {
            let reference = VenueReference {
                ticker: Some(ticker),
                ..equity()
            };
            let decision = route_venue("SYNTHETIC", &reference, &HashMap::new());
            assert_eq!(decision.venue_topic, Some(format!("{ticker} UN Equity")));
            assert_eq!(decision.method, Some(VenueMethod::ExchangeTicker));
        }
    }
}
