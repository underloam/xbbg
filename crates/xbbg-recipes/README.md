# xbbg-recipes

High-level recipe functions built on `xbbg-async` and `xbbg-ext`.

This crate groups reusable market-data workflows such as:

- fixed-income analytics helpers
- futures helper workflows
- historical data transformations
- common error types for recipe-level APIs

The crate is independent and uses the shared xbbg Rust engine for BLPAPI-backed request execution when the `live` feature is enabled. Runtime use requires separately licensed service access and local BLPAPI runtime components where applicable.

## Shared workflows

Python extension helpers and the Node binding execute the Rust recipes rather than
reimplementing their request orchestration and transformations. Migrated recipes
accept a final `RequestParams` value for overrides, request elements, field types,
entitlement IDs, validation, timezones, and supported output formats. Pass
`RequestParams::default()` for the default controls. Recipe-owned securities,
fields, services, and operations cannot be replaced by these controls.
Per-security overrides apply only to matching securities in each internal
request. Historical `adjust` shorthand is translated natively; obsolete bulk
`raw` controls are consumed rather than sent to Bloomberg.

- `recipe_earning` accepts statement types `IS`/`BS`/`CF` and geography/product
  metrics (`Revenue`, `Operating_Income`, `Assets`, `Gross_Profit`,
  `Capital_Expenditures`). `by` accepts `Q`/`A` or `Geo`/`Product`; fiscal year
  and period-count overrides are available through the shared native interface.
- `recipe_adjust_ccy` converts wide ticker columns or long ticker/date/value
  data. Long text values remain text; converted numeric values become Float64.
  Categorical inputs are decoded, and timestamp lookups use their local calendar
  dates. Missing or invalid FX rates produce nulls instead of leaving converted
  rows in local currency. Request failures propagate. `local` is an identity
  conversion.
- Futures resolution adds historical bulk-chain fallback to native candidate
  selection. The active recipe prefers current-contract metadata and otherwise
  compares the latest non-null volumes over ten days.
- BQR requests default to the previous hour through now in UTC and return sorted
  quote columns (`event_type`, `price`, and broker attribution when requested).

The Rust `recipe_cdx_ticker` and `recipe_active_cdx` convenience functions have
been removed. Call `recipe_cdx_ticker_with_options` and
`recipe_active_cdx_with_options`, passing `versionless = false` for the former
defaults. `clear_venue_cache` remains available.

Python helper names and signatures are unchanged, but recipe validation, column
names, selection rules, and error propagation now follow the shared Rust
implementation. Native tabular results are converted to the configured Python
backend; a call's `backend` option takes precedence.
