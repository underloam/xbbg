//! xbbg-recipes: High-level Bloomberg recipe functions.
//!
//! Recipes are composed workflows built on top of the xbbg-async engine.
//! Each recipe function takes an Engine reference and performs one or more
//! Bloomberg API calls to produce a final Arrow RecordBatch.
//!
//! Architecture: Recipes call engine.request() directly (no recursion).
//! Recipes are a layer ABOVE the engine, not part of it.

pub mod auction;
pub mod currency;
pub mod error;
pub mod etf;
pub mod fixed_income;
pub mod futures;
pub mod historical;
pub mod identifiers;
pub mod indices;
pub mod utils;
pub mod volatility;

pub use auction::{clear_venue_cache, recipe_auction_snapshot, recipe_resolve_venues};
pub use error::{RecipeError, Result};
pub use utils::*;
