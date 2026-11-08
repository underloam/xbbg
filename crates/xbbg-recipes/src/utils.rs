//! Shared utility functions used across recipe modules.
use std::borrow::Cow;
use std::collections::HashMap;

use chrono::NaiveDate;

use arrow_array::{
    Array, ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, LargeStringArray,
    RecordBatch, StringArray, StringViewArray,
};
use xbbg_async::engine::{ExtractorType, RequestParams};
use xbbg_async::services::{Operation, Service};

use crate::error::{RecipeError, Result};

/// Merge caller request controls without changing the recipe's request identity.
///
/// Explicit pairs replace recipe defaults by name. BQL uses elements rather
/// than field overrides; raw kwargs remain available to the engine's router.
pub(crate) fn apply_request_options(params: &mut RequestParams, options: &RequestParams) {
    if params.service == Service::BqlSvc.as_str() {
        merge_pairs(&mut params.elements, options.overrides.as_deref());
    } else {
        merge_pairs(&mut params.overrides, options.overrides.as_deref());
    }
    merge_pairs(&mut params.elements, options.elements.as_deref());
    if params.operation == Operation::HistoricalData.as_str() {
        let adjustment = options
            .kwargs
            .as_ref()
            .and_then(|values| values.get("adjust"));
        let fields: &[&str] = match adjustment.map(String::as_str) {
            Some("all") => &["adjustmentSplit", "adjustmentNormal", "adjustmentAbnormal"],
            Some("dvd") => &["adjustmentNormal", "adjustmentAbnormal"],
            Some("split") => &["adjustmentSplit"],
            _ => &[],
        };
        for field in fields {
            let values = params.options.get_or_insert_with(Vec::new);
            if let Some((_, value)) = values
                .iter_mut()
                .find(|(key, _)| key.eq_ignore_ascii_case(field))
            {
                value.clear();
                value.push_str("true");
            } else {
                values.push(((*field).to_string(), "true".to_string()));
            }
        }
    }
    merge_pairs(&mut params.options, options.options.as_deref());
    if let Some(values) = &options.kwargs {
        params
            .kwargs
            .get_or_insert_with(Default::default)
            .extend(values.clone());
    }
    if let Some(kwargs) = &mut params.kwargs {
        // Historical display/adjustment controls do not belong in metadata lookups.
        kwargs.remove("adjust");
        if params.extractor == ExtractorType::BulkData {
            kwargs.remove("raw");
        }
    }
    if let Some(values) = &options.security_overrides {
        for (security, overrides) in values {
            if params
                .securities
                .as_ref()
                .is_some_and(|values| values.contains(security))
                || params.security.as_ref() == Some(security)
            {
                params
                    .security_overrides
                    .get_or_insert_with(Vec::new)
                    .push((security.clone(), overrides.clone()));
            }
        }
    }
    if let Some(values) = &options.field_types {
        params
            .field_types
            .get_or_insert_with(Default::default)
            .extend(values.clone());
    }
    if options.format.is_some() {
        params.format.clone_from(&options.format);
    }
    if params.extractor == ExtractorType::BulkData {
        params.format = None;
    }
    if options.request_tz.is_some() {
        params.request_tz.clone_from(&options.request_tz);
    }
    if options.output_tz.is_some() {
        params.output_tz.clone_from(&options.output_tz);
    }
    if options.request_id.is_some() {
        params.request_id.clone_from(&options.request_id);
    }
    if options.validate_fields.is_some() {
        params.validate_fields = options.validate_fields;
    }
    params.include_security_errors |= options.include_security_errors;
    params.return_eids |= options.return_eids;
}

fn merge_pairs(target: &mut Option<Vec<(String, String)>>, source: Option<&[(String, String)]>) {
    let Some(source) = source.filter(|values| !values.is_empty()) else {
        return;
    };
    let target = target.get_or_insert_with(Vec::new);
    for (key, value) in source {
        if let Some((_, current)) = target
            .iter_mut()
            .find(|(name, _)| name.eq_ignore_ascii_case(key))
        {
            current.clone_from(value);
        } else {
            target.push((key.clone(), value.clone()));
        }
    }
}

/// Typed read view over one Arrow column, resolved once per column.
///
/// Recipe loops resolve the concrete array type up front instead of running a
/// downcast chain per cell. Each accessor returns exactly what the matching
/// `array_value_as_*` function returns for the same column and row.
#[derive(Clone, Copy)]
pub(crate) struct ColumnValues<'a> {
    len: usize,
    kind: ColumnKind<'a>,
}

#[derive(Clone, Copy)]
enum ColumnKind<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Float64(&'a Float64Array),
    Int64(&'a Int64Array),
    Int32(&'a Int32Array),
    Date32(&'a Date32Array),
    Unsupported,
}

impl<'a> ColumnValues<'a> {
    /// Resolve the concrete array type of `array`.
    pub(crate) fn new(array: &'a dyn Array) -> Self {
        let any = array.as_any();
        let kind = if let Some(arr) = any.downcast_ref::<StringArray>() {
            ColumnKind::Utf8(arr)
        } else if let Some(arr) = any.downcast_ref::<LargeStringArray>() {
            ColumnKind::LargeUtf8(arr)
        } else if let Some(arr) = any.downcast_ref::<Float64Array>() {
            ColumnKind::Float64(arr)
        } else if let Some(arr) = any.downcast_ref::<Int64Array>() {
            ColumnKind::Int64(arr)
        } else if let Some(arr) = any.downcast_ref::<Int32Array>() {
            ColumnKind::Int32(arr)
        } else if let Some(arr) = any.downcast_ref::<Date32Array>() {
            ColumnKind::Date32(arr)
        } else {
            ColumnKind::Unsupported
        };
        Self {
            len: array.len(),
            kind,
        }
    }

    /// Whether `idx` is in range and non-null for a supported array type.
    fn present(&self, idx: usize) -> bool {
        if idx >= self.len {
            return false;
        }
        match self.kind {
            ColumnKind::Utf8(arr) => arr.is_valid(idx),
            ColumnKind::LargeUtf8(arr) => arr.is_valid(idx),
            ColumnKind::Float64(arr) => arr.is_valid(idx),
            ColumnKind::Int64(arr) => arr.is_valid(idx),
            ColumnKind::Int32(arr) => arr.is_valid(idx),
            ColumnKind::Date32(arr) => arr.is_valid(idx),
            ColumnKind::Unsupported => false,
        }
    }

    /// Whether the column is `Utf8` or `LargeUtf8`.
    pub(crate) fn is_text(&self) -> bool {
        matches!(self.kind, ColumnKind::Utf8(_) | ColumnKind::LargeUtf8(_))
    }

    /// Raw borrowed text at `idx`; `None` for null rows and non-text columns.
    pub(crate) fn text(&self, idx: usize) -> Option<&'a str> {
        if !self.present(idx) {
            return None;
        }
        match self.kind {
            ColumnKind::Utf8(arr) => Some(arr.value(idx)),
            ColumnKind::LargeUtf8(arr) => Some(arr.value(idx)),
            _ => None,
        }
    }

    /// Value at `idx` as text, borrowed for string columns.
    ///
    /// Same result as [`array_value_as_string`].
    pub(crate) fn string(&self, idx: usize) -> Option<Cow<'a, str>> {
        if !self.present(idx) {
            return None;
        }
        match self.kind {
            ColumnKind::Utf8(arr) => Some(Cow::Borrowed(arr.value(idx))),
            ColumnKind::LargeUtf8(arr) => Some(Cow::Borrowed(arr.value(idx))),
            ColumnKind::Float64(arr) => Some(Cow::Owned(arr.value(idx).to_string())),
            ColumnKind::Int64(arr) => Some(Cow::Owned(arr.value(idx).to_string())),
            ColumnKind::Int32(arr) => Some(Cow::Owned(arr.value(idx).to_string())),
            ColumnKind::Date32(arr) => date32_to_naive(arr.value(idx))
                .map(|date| Cow::Owned(date.format("%Y-%m-%d").to_string())),
            ColumnKind::Unsupported => None,
        }
    }

    /// Value at `idx` as `f64`, parsing text with [`parse_f64_like`].
    ///
    /// Same result as [`array_value_as_f64`].
    pub(crate) fn f64(&self, idx: usize) -> Option<f64> {
        if !self.present(idx) {
            return None;
        }
        match self.kind {
            ColumnKind::Float64(arr) => Some(arr.value(idx)),
            ColumnKind::Int64(arr) => Some(arr.value(idx) as f64),
            ColumnKind::Int32(arr) => Some(arr.value(idx) as f64),
            ColumnKind::Utf8(arr) => parse_f64_like(arr.value(idx)),
            ColumnKind::LargeUtf8(arr) => parse_f64_like(arr.value(idx)),
            ColumnKind::Date32(_) | ColumnKind::Unsupported => {
                self.string(idx).and_then(|value| parse_f64_like(&value))
            }
        }
    }

    /// Value at `idx` as a date, parsing text with [`parse_any_date`].
    ///
    /// Same result as [`array_value_as_date`].
    pub(crate) fn date(&self, idx: usize) -> Option<NaiveDate> {
        if !self.present(idx) {
            return None;
        }
        match self.kind {
            ColumnKind::Date32(arr) => date32_to_naive(arr.value(idx)),
            ColumnKind::Utf8(arr) => parse_any_date(arr.value(idx)),
            ColumnKind::LargeUtf8(arr) => parse_any_date(arr.value(idx)),
            ColumnKind::Float64(_)
            | ColumnKind::Int64(_)
            | ColumnKind::Int32(_)
            | ColumnKind::Unsupported => self.string(idx).and_then(|value| parse_any_date(&value)),
        }
    }
}

/// Borrowed view over a Utf8, LargeUtf8 or Utf8View column, resolved once.
///
/// Rows of any other array type read as `None`.
#[derive(Clone, Copy)]
pub(crate) enum TextValues<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
    Other,
}

impl<'a> TextValues<'a> {
    /// Resolve the concrete string array type of `array`.
    pub(crate) fn new(array: &'a dyn Array) -> Self {
        let any = array.as_any();
        if let Some(arr) = any.downcast_ref::<StringArray>() {
            Self::Utf8(arr)
        } else if let Some(arr) = any.downcast_ref::<LargeStringArray>() {
            Self::LargeUtf8(arr)
        } else if let Some(arr) = any.downcast_ref::<StringViewArray>() {
            Self::Utf8View(arr)
        } else {
            Self::Other
        }
    }

    /// Text at `row`, or `None` when the row is null or the column is not text.
    pub(crate) fn get(&self, row: usize) -> Option<&'a str> {
        match *self {
            Self::Utf8(arr) => arr.is_valid(row).then(|| arr.value(row)),
            Self::LargeUtf8(arr) => arr.is_valid(row).then(|| arr.value(row)),
            Self::Utf8View(arr) => arr.is_valid(row).then(|| arr.value(row)),
            Self::Other => None,
        }
    }
}

/// Mutable entry for a borrowed string key, allocating the owned key only on
/// first insertion (rows of a long batch repeat the same ticker).
pub(crate) fn str_entry<'m, V: Default>(map: &'m mut HashMap<String, V>, key: &str) -> &'m mut V {
    if !map.contains_key(key) {
        map.insert(key.to_owned(), V::default());
    }
    map.get_mut(key).expect("entry inserted above")
}

/// Extract a value from an Arrow array at `idx` as a `String`.
///
/// Supports `StringArray`, `LargeStringArray`, numeric arrays, and `Date32Array`.
/// Returns `None` for null values, out-of-bounds indices, or unsupported array
/// types.
pub fn array_value_as_string(array: &ArrayRef, idx: usize) -> Option<String> {
    ColumnValues::new(array.as_ref())
        .string(idx)
        .map(Cow::into_owned)
}

/// Convert a `Date32` value (days since Unix epoch) to a `chrono::NaiveDate`.
pub fn date32_to_naive(days_since_epoch: i32) -> Option<chrono::NaiveDate> {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    epoch.checked_add_signed(chrono::Duration::days(days_since_epoch as i64))
}

/// Borrow a column from a `RecordBatch` as a `&StringArray`, returning an
/// error if the column is missing or is not `Utf8`.
pub fn as_string_col<'a>(batch: &'a RecordBatch, column: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(column)
        .ok_or_else(|| RecipeError::Other(format!("missing '{column}' column")))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| RecipeError::Other(format!("'{column}' column must be Utf8")))
}

/// Clean a Bloomberg text value, mapping blank and sentinel values
/// (`nan`, `n/a`, `#n/a`, `null`, case-insensitive) to `None`.
pub(crate) fn clean_bloomberg_text(value: &str) -> Option<String> {
    let text = value.trim();
    if text.is_empty()
        || text.eq_ignore_ascii_case("nan")
        || text.eq_ignore_ascii_case("n/a")
        || text.eq_ignore_ascii_case("#n/a")
        || text.eq_ignore_ascii_case("null")
    {
        None
    } else {
        Some(text.to_string())
    }
}

/// Return a lowercase alphanumeric key for matching Bloomberg sub-field labels.
pub fn canonical_name(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .split('_')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("_")
}

/// Find the first column whose canonical label matches one of `candidates`.
pub fn find_column(batch: &RecordBatch, candidates: &[&str]) -> Option<String> {
    let wanted = candidates
        .iter()
        .map(|candidate| canonical_name(candidate))
        .collect::<Vec<_>>();

    batch.schema().fields().iter().find_map(|field| {
        let key = canonical_name(field.name());
        wanted
            .iter()
            .any(|candidate| candidate == &key)
            .then(|| field.name().to_string())
    })
}

/// Convert a `NaiveDate` to Arrow Date32 days since Unix epoch.
pub fn naive_to_date32(date: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid unix epoch");
    (date - epoch).num_days() as i32
}

/// Parse common Bloomberg date string representations.
pub fn parse_any_date(value: &str) -> Option<NaiveDate> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }

    for fmt in ["%Y%m%d", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"] {
        if let Ok(date) = NaiveDate::parse_from_str(value, fmt) {
            return Some(date);
        }
    }

    value
        .get(..10)
        .and_then(|prefix| NaiveDate::parse_from_str(prefix, "%Y-%m-%d").ok())
}

/// Extract a value from an Arrow array as `f64`.
pub fn array_value_as_f64(array: &ArrayRef, idx: usize) -> Option<f64> {
    ColumnValues::new(array.as_ref()).f64(idx)
}

/// Extract a value from an Arrow array as `NaiveDate`.
pub fn array_value_as_date(array: &ArrayRef, idx: usize) -> Option<NaiveDate> {
    ColumnValues::new(array.as_ref()).date(idx)
}

/// Parse a Bloomberg numeric string, allowing comma thousands separators.
pub fn parse_f64_like(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    if trimmed.as_bytes().contains(&b',') {
        let mut cleaned = String::with_capacity(trimmed.len());
        cleaned.extend(trimmed.chars().filter(|ch| *ch != ','));
        cleaned
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
    } else {
        trimmed
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        BooleanArray, Date32Array, Date64Array, Float64Array, NullArray, StringArray,
        TimestampMillisecondArray,
    };

    use super::*;

    #[test]
    fn array_value_as_string_formats_typed_values() {
        let string_values: ArrayRef = Arc::new(StringArray::from(vec![Some("abc")]));
        let numeric_values: ArrayRef = Arc::new(Float64Array::from(vec![Some(12.5)]));
        let date_values: ArrayRef = Arc::new(Date32Array::from(vec![Some(naive_to_date32(
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
        ))]));

        assert_eq!(
            array_value_as_string(&string_values, 0).as_deref(),
            Some("abc")
        );
        assert_eq!(
            array_value_as_string(&numeric_values, 0).as_deref(),
            Some("12.5")
        );
        assert_eq!(
            array_value_as_string(&date_values, 0).as_deref(),
            Some("2024-01-02")
        );
    }

    #[test]
    fn parse_f64_like_parses_plain_and_grouped_numbers() {
        assert_eq!(parse_f64_like("123.45"), Some(123.45));
        assert_eq!(parse_f64_like("1,234.5"), Some(1234.5));
        assert_eq!(parse_f64_like("nan"), None);
    }

    // Per-cell implementations the column view replaced; kept as the parity oracle.
    fn legacy_string(array: &ArrayRef, idx: usize) -> Option<String> {
        if idx >= array.len() || array.is_null(idx) {
            return None;
        }
        let any = array.as_any();
        if let Some(arr) = any.downcast_ref::<StringArray>() {
            return Some(arr.value(idx).to_string());
        }
        if let Some(arr) = any.downcast_ref::<LargeStringArray>() {
            return Some(arr.value(idx).to_string());
        }
        if let Some(arr) = any.downcast_ref::<Float64Array>() {
            return Some(arr.value(idx).to_string());
        }
        if let Some(arr) = any.downcast_ref::<Int64Array>() {
            return Some(arr.value(idx).to_string());
        }
        if let Some(arr) = any.downcast_ref::<Int32Array>() {
            return Some(arr.value(idx).to_string());
        }
        if let Some(arr) = any.downcast_ref::<Date32Array>() {
            return date32_to_naive(arr.value(idx)).map(|date| date.format("%Y-%m-%d").to_string());
        }
        None
    }

    fn legacy_f64(array: &ArrayRef, idx: usize) -> Option<f64> {
        if idx >= array.len() || array.is_null(idx) {
            return None;
        }
        let any = array.as_any();
        if let Some(arr) = any.downcast_ref::<Float64Array>() {
            return Some(arr.value(idx));
        }
        if let Some(arr) = any.downcast_ref::<Int64Array>() {
            return Some(arr.value(idx) as f64);
        }
        if let Some(arr) = any.downcast_ref::<Int32Array>() {
            return Some(arr.value(idx) as f64);
        }
        legacy_string(array, idx).and_then(|value| parse_f64_like(&value))
    }

    fn legacy_date(array: &ArrayRef, idx: usize) -> Option<NaiveDate> {
        if idx >= array.len() || array.is_null(idx) {
            return None;
        }
        if let Some(arr) = array.as_any().downcast_ref::<Date32Array>() {
            return date32_to_naive(arr.value(idx));
        }
        legacy_string(array, idx).and_then(|value| parse_any_date(&value))
    }

    #[test]
    fn column_view_matches_per_cell_extraction_for_every_array_type() {
        let text = [
            Some("abc"),
            Some(" 1,234.5 "),
            Some("20240102"),
            Some("2024-01-02T10:00:00"),
            Some("01/31/2024"),
            Some("nan"),
            Some(""),
            None,
        ];
        let date = naive_to_date32(NaiveDate::from_ymd_opt(2024, 1, 2).unwrap());
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(text.to_vec())),
            Arc::new(LargeStringArray::from(text.to_vec())),
            Arc::new(Float64Array::from(vec![
                Some(12.5),
                Some(20_240_102.0),
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(-0.0),
                None,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(20_240_102),
                Some(-5),
                Some(i64::MAX),
                None,
            ])),
            Arc::new(Int32Array::from(vec![Some(7), Some(20_240_102), None])),
            Arc::new(Date32Array::from(vec![Some(date), Some(i32::MAX), None])),
            // Unsupported types read as absent, exactly as before.
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Arc::new(StringViewArray::from(vec![Some("2024-01-02"), None])),
            Arc::new(Date64Array::from(vec![Some(1_704_153_600_000), None])),
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(1_704_153_600_000),
                None,
            ])),
            Arc::new(NullArray::new(2)),
        ];

        for array in &arrays {
            let view = ColumnValues::new(array.as_ref());
            for idx in 0..array.len() + 2 {
                let context = format!("{:?} row {idx}", array.data_type());
                assert_eq!(
                    view.string(idx).map(Cow::into_owned),
                    legacy_string(array, idx),
                    "string {context}"
                );
                assert_eq!(
                    view.f64(idx).map(f64::to_bits),
                    legacy_f64(array, idx).map(f64::to_bits),
                    "f64 {context}"
                );
                assert_eq!(view.date(idx), legacy_date(array, idx), "date {context}");
                assert_eq!(
                    array_value_as_string(array, idx),
                    legacy_string(array, idx),
                    "array_value_as_string {context}"
                );
                assert_eq!(
                    array_value_as_f64(array, idx).map(f64::to_bits),
                    legacy_f64(array, idx).map(f64::to_bits),
                    "array_value_as_f64 {context}"
                );
                assert_eq!(
                    array_value_as_date(array, idx),
                    legacy_date(array, idx),
                    "array_value_as_date {context}"
                );
            }
        }
    }

    #[test]
    fn column_view_borrows_text_cells() {
        let array: ArrayRef = Arc::new(StringArray::from(vec![Some("IBM US Equity")]));
        assert!(matches!(
            ColumnValues::new(array.as_ref()).string(0),
            Some(Cow::Borrowed("IBM US Equity"))
        ));
    }

    #[test]
    fn text_view_reads_all_string_layouts_and_skips_other_types() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![Some("a"), None])),
            Arc::new(LargeStringArray::from(vec![Some("a"), None])),
            Arc::new(StringViewArray::from(vec![Some("a"), None])),
        ];
        for array in &arrays {
            let view = TextValues::new(array.as_ref());
            assert_eq!(view.get(0), Some("a"));
            assert_eq!(view.get(1), None);
        }
        let numbers: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.0)]));
        assert_eq!(TextValues::new(numbers.as_ref()).get(0), None);
    }

    #[test]
    fn recipe_options_merge_without_replacing_request_identity() {
        let mut params = RequestParams {
            service: Service::RefData.to_string(),
            operation: "ReferenceDataRequest".into(),
            securities: Some(vec!["ABC US Equity".into()]),
            fields: Some(vec!["PX_LAST".into()]),
            overrides: Some(vec![("SETTLE_DT".into(), "20240101".into())]),
            ..Default::default()
        };
        let options = RequestParams {
            service: "//ignored".into(),
            fields: Some(vec!["IGNORED".into()]),
            overrides: Some(vec![
                ("settle_dt".into(), "20240102".into()),
                ("CUSTOM".into(), "1".into()),
            ]),
            format: Some("wide".into()),
            validate_fields: Some(false),
            ..Default::default()
        };
        apply_request_options(&mut params, &options);
        assert_eq!(params.service, Service::RefData.to_string());
        assert_eq!(params.fields.unwrap(), vec!["PX_LAST"]);
        assert_eq!(params.securities.unwrap(), vec!["ABC US Equity"]);
        assert_eq!(
            params.overrides.unwrap(),
            vec![
                ("SETTLE_DT".into(), "20240102".into()),
                ("CUSTOM".into(), "1".into())
            ]
        );
        assert_eq!(params.format.as_deref(), Some("wide"));
        assert_eq!(params.validate_fields, Some(false));
    }

    #[test]
    fn bql_recipe_options_route_overrides_to_elements() {
        let mut params = RequestParams {
            service: Service::BqlSvc.to_string(),
            elements: Some(vec![(
                "expression".into(),
                "get(id) for('ABC US Equity')".into(),
            )]),
            ..Default::default()
        };
        apply_request_options(
            &mut params,
            &RequestParams {
                overrides: Some(vec![("currency".into(), "USD".into())]),
                ..Default::default()
            },
        );
        assert!(params.overrides.is_none());
        assert_eq!(
            params.elements.unwrap()[1],
            ("currency".into(), "USD".into())
        );
    }

    #[test]
    fn adjustment_controls_only_reach_historical_requests() {
        for (adjust, expected) in [
            (
                "all",
                vec!["adjustmentSplit", "adjustmentNormal", "adjustmentAbnormal"],
            ),
            ("dvd", vec!["adjustmentNormal", "adjustmentAbnormal"]),
            ("split", vec!["adjustmentSplit"]),
        ] {
            let options = RequestParams {
                kwargs: Some(std::collections::HashMap::from([(
                    "adjust".into(),
                    adjust.into(),
                )])),
                ..Default::default()
            };
            let mut history = RequestParams {
                operation: Operation::HistoricalData.to_string(),
                ..Default::default()
            };
            apply_request_options(&mut history, &options);
            assert_eq!(
                history.options.unwrap(),
                expected
                    .into_iter()
                    .map(|name| (name.to_string(), "true".into()))
                    .collect::<Vec<_>>()
            );
            assert!(!history.kwargs.unwrap().contains_key("adjust"));
            let mut reference = RequestParams::default();
            apply_request_options(&mut reference, &options);
            assert!(reference.options.is_none());
            assert!(!reference.kwargs.unwrap().contains_key("adjust"));
        }
    }

    #[test]
    fn explicit_adjustment_options_win_over_legacy_shorthand() {
        let mut history = RequestParams {
            operation: Operation::HistoricalData.to_string(),
            ..Default::default()
        };
        apply_request_options(
            &mut history,
            &RequestParams {
                kwargs: Some(std::collections::HashMap::from([(
                    "adjust".into(),
                    "all".into(),
                )])),
                options: Some(vec![("adjustmentSplit".into(), "false".into())]),
                ..Default::default()
            },
        );
        assert_eq!(
            history.options.unwrap()[0],
            ("adjustmentSplit".into(), "false".into())
        );
    }

    #[test]
    fn bulk_requests_consume_raw_and_cannot_select_output_format() {
        let mut bulk = RequestParams {
            extractor: ExtractorType::BulkData,
            extractor_set: true,
            ..Default::default()
        };
        apply_request_options(
            &mut bulk,
            &RequestParams {
                kwargs: Some(std::collections::HashMap::from([(
                    "raw".into(),
                    "True".into(),
                )])),
                format: Some("wide".into()),
                ..Default::default()
            },
        );
        assert!(!bulk.kwargs.unwrap().contains_key("raw"));
        assert!(bulk.format.is_none());
    }

    #[test]
    fn per_security_overrides_follow_each_internal_request_security_set() {
        let options = RequestParams {
            security_overrides: Some(vec![
                ("ABC LN Equity".into(), vec![("CUSTOM".into(), "1".into())]),
                ("USDGBP Curncy".into(), vec![("CUSTOM".into(), "2".into())]),
            ]),
            ..Default::default()
        };
        for (security, expected) in [("ABC LN Equity", "1"), ("USDGBP Curncy", "2")] {
            let mut params = RequestParams {
                securities: Some(vec![security.into()]),
                ..Default::default()
            };
            apply_request_options(&mut params, &options);
            assert_eq!(
                params.security_overrides.unwrap(),
                vec![(security.into(), vec![("CUSTOM".into(), expected.into())])]
            );
        }
        let mut params = RequestParams {
            securities: Some(vec!["OTHER US Equity".into()]),
            ..Default::default()
        };
        apply_request_options(&mut params, &options);
        assert!(params.security_overrides.is_none());
    }
}
