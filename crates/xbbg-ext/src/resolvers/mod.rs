//! Ticker resolution utilities for futures and CDX contracts.

pub mod cdx;
pub mod futures;

pub use cdx::{
    CdxInfo, CdxVersion, ParsedCdxInfo, ResolvedCdxInfo, cdx_series_from_ticker, parse_cdx_ticker,
};
pub use futures::{
    FuturesCandidate, RollFrequency, filter_valid_contracts, generate_futures_candidates,
};
