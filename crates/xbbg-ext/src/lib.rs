//! xbbg-ext: Extension utilities for xbbg
//!
//! This crate provides high-performance Rust implementations of common
//! Bloomberg data transformations and utilities:
//!
//! - **auction**: Stream-valid auction fields and validated venue-routing decisions
//! - **constants**: Compile-time maps for futures months, dividend types, etc.
//! - **utils**: Date parsing, DataFrame pivoting, ticker normalization
//! - **resolvers**: Futures and CDX ticker resolution
//! - **transforms**: Currency adjustment, historical data processing

pub mod auction;
pub mod constants;
pub mod error;
pub mod markets;
pub mod resolvers;
pub mod transforms;
pub mod utils;

pub use error::{ExtError, Result};

// Re-export commonly used items
pub use auction::{
    equity_venue_ticker, field_group, field_group_names, imbalance_side, is_valid_isin,
    normalize_security_input, pcs_isin_topic, pfd_pricing_source, route_venue, validate_venue,
    ImbalanceSide, VenueDecision, VenueMethod, VenueReference, VenueStatus,
};
pub use constants::{DVD_COLS, DVD_TYPES, ETF_COLS, FUTURES_MONTHS, MONTH_CODES};
pub use markets::sessions::{
    derive_sessions, get_market_rule, infer_timezone_from_country, MarketRule, SessionWindows,
};
pub use markets::{
    clear_exchange_override, get_exchange_override, get_exchange_override_patch,
    has_exchange_override, list_exchange_overrides, market_timing, session_times_to_utc,
    set_exchange_override, ExchangeInfo, ExchangeInfoSource, MarketInfo, MarketTiming,
    OverridePatch,
};
pub use resolvers::futures::filter_valid_contracts;
pub use transforms::historical::build_earning_header_rename;
pub use transforms::{
    build_corporate_bonds_query, build_etf_holdings_query, build_preferreds_query,
};
pub use transforms::{build_yas_overrides, YieldType};
pub use utils::date::{fmt_date, parse_date};
pub use utils::pivot::pivot_to_wide;
pub use utils::ticker::{normalize_tickers, parse_ticker_parts, TickerParts};
