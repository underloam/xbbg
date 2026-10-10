//! Pure Arrow carrier helpers shared by xbbg native bindings.
//!
//! This crate intentionally has no Python, NAPI, Bloomberg SDK, or dataframe
//! dependencies. Binding crates own language-specific conversion and optional
//! backend imports; this crate owns small Arrow table, batch, column, and scalar
//! operations.

pub mod column;
pub mod error;
pub mod scalar;
pub mod table;

pub use column::ColumnData;
pub use error::{ArrowCoreError, Result};
pub use scalar::{
    CellValue, build_array, cell_from_array, cell_has_value, cell_matches, cell_to_string,
};
pub use table::{SortDirection, TableData};
