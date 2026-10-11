//! BQL (Bloomberg Query Language) state with Arrow builders.
//!
//! JSON payloads use a borrowing typed parser through 32 KiB and
//! `serde_json::Value` above that threshold. Both feed one borrowed-view Arrow
//! materializer; native Bloomberg Elements retain the fallback path.
//!
//! Results become a table with a ticker column, secondary dimensions, and one
//! value column per requested field.

use arrow_array::RecordBatch;
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::Value as JsonValue;
use std::{borrow::Cow, collections::BTreeMap, marker::PhantomData};
use tokio::sync::oneshot;

use super::typed_builder::ColumnSet;
use super::value_utils::top_level_response_error;
use xbbg_core::{BlpError, Message};

mod materialize;

const BQL_TYPED_JSON_MAX_BYTES: usize = 32 * 1024;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BqlJsonResponse<'a> {
    #[serde(default, borrow)]
    client_context: Option<BqlClientContext<'a>>,
    #[serde(default, borrow)]
    response_exceptions: Option<Vec<BqlException<'a>>>,
    #[serde(default, borrow)]
    results: Option<BTreeMap<String, BqlJsonField<'a>>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BqlClientContext<'a> {
    #[serde(default, borrow)]
    client_request_id: Option<Cow<'a, str>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BqlJsonField<'a> {
    #[serde(default, borrow)]
    id_column: Option<BqlJsonColumn<'a>>,
    #[serde(default, borrow)]
    values_column: Option<BqlJsonColumn<'a>>,
    #[serde(default, borrow)]
    secondary_columns: Vec<BqlJsonColumn<'a>>,
    #[serde(default, borrow)]
    response_exceptions: Option<Vec<BqlException<'a>>>,
}

#[derive(Debug, Deserialize)]
struct BqlJsonColumn<'a> {
    #[serde(default, borrow)]
    name: Option<Cow<'a, str>>,
    #[serde(default, rename = "type", borrow)]
    data_type: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    values: Vec<BqlCell<'a>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BqlException<'a> {
    #[serde(default, borrow)]
    message: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    node_name: Option<Cow<'a, str>>,
}

#[derive(Clone, Copy, Debug)]
enum BqlNumber {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
}

#[derive(Debug)]
enum BqlCell<'a> {
    String(Cow<'a, str>),
    Number(BqlNumber),
    Bool(bool),
    Null,
    Other(Box<JsonValue>),
}

struct BqlCellVisitor<'a>(PhantomData<&'a str>);

impl<'de, 'a> Visitor<'de> for BqlCellVisitor<'a>
where
    'de: 'a,
{
    type Value = BqlCell<'a>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON string, number, boolean, null, array, or object")
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E> {
        Ok(BqlCell::String(Cow::Borrowed(value)))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(BqlCell::String(Cow::Owned(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(BqlCell::String(Cow::Owned(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(BqlCell::Number(BqlNumber::Signed(value)))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(BqlCell::Number(BqlNumber::Unsigned(value)))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
        Ok(BqlCell::Number(BqlNumber::Float(value)))
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(BqlCell::Bool(value))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(BqlCell::Null)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(BqlCell::Null)
    }

    fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        JsonValue::deserialize(de::value::SeqAccessDeserializer::new(seq))
            .map(Box::new)
            .map(BqlCell::Other)
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        JsonValue::deserialize(de::value::MapAccessDeserializer::new(map))
            .map(Box::new)
            .map(BqlCell::Other)
    }
}

impl<'de, 'a> Deserialize<'de> for BqlCell<'a>
where
    'de: 'a,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(BqlCellVisitor(PhantomData))
    }
}

/// State for a BQL request.
pub struct BqlState {
    /// Column set for building the output
    columns: ColumnSet,
    /// Reply channel
    pub reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
    /// Accumulated JSON string (for JSON-encoded responses)
    json_buffer: Option<String>,
}

impl BqlState {
    /// Create a new BQL state.
    pub fn new(reply: oneshot::Sender<Result<RecordBatch, BlpError>>) -> Self {
        Self {
            columns: ColumnSet::new(),
            reply,
            json_buffer: None,
        }
    }

    /// Process a PARTIAL_RESPONSE message.
    pub fn on_partial(&mut self, msg: &Message) {
        self.process_message(msg);
    }

    /// Process the final RESPONSE message and send the result via reply channel.
    pub fn finish(mut self, msg: &Message) {
        if let Some(error) = top_level_response_error(msg, "//blp/bqlsvc", "sendQuery") {
            let _ = self.reply.send(Err(error));
            return;
        }

        self.process_message(msg);

        // If we accumulated JSON, try to parse it
        let result = if let Some(json_str) = self.json_buffer.take() {
            self.parse_bql_json(&json_str)
        } else {
            self.columns.finish()
        };

        if let Ok(ref batch) = result {
            xbbg_log::debug!(
                rows = batch.num_rows(),
                cols = batch.num_columns(),
                "bql finish"
            );
        }
        let _ = self.reply.send(result);
    }

    /// Process a BQL response message using Element API.
    ///
    /// BQL response structure:
    /// ```text
    /// beqlData {
    ///   results[] {
    ///     ... varies by query
    ///   }
    /// }
    /// ```
    fn process_message(&mut self, msg: &Message) {
        let root = msg.elements();

        // Try different BQL response structures
        // Structure 1: beqlData -> results
        if let Some(beql_data) = root.get_by_str("beqlData") {
            if let Some(results) = beql_data.get_by_str("results") {
                // Check if first result is a JSON string
                if !results.is_empty()
                    && let Some(first) = results.get_element(0)
                    && let Some(xbbg_core::Value::String(s)) = first.get_value(0)
                {
                    // This is a JSON-encoded response
                    if s.starts_with('{') {
                        self.json_buffer = Some(s.to_string());
                        return;
                    }
                }
                self.extract_results(&results);
                return;
            }

            // Check for direct JSON string in beqlData
            if let Some(xbbg_core::Value::String(s)) = beql_data.get_value(0)
                && s.starts_with('{')
            {
                self.json_buffer = Some(s.to_string());
                return;
            }
        }

        // Structure 2: Direct results array
        if let Some(results) = root.get_by_str("results") {
            self.extract_results(&results);
            return;
        }

        // Structure 3: Check if root contains a JSON string value
        if let Some(xbbg_core::Value::String(s)) = root.get_value(0)
            && s.starts_with('{')
        {
            self.json_buffer = Some(s.to_string());
            return;
        }

        // Structure 4: Flatten the entire response (fallback)
        self.flatten_element("", &root);
    }

    /// Parse BQL JSON response into a proper table.
    ///
    /// Bloomberg BQL JSON response structure:
    /// ```json
    /// {
    ///   "clientContext": { "clientRequestId": "...", ... },
    ///   "responseExceptions": null | [{ "message", "messageCategory",
    ///       "messageSubcategory", "nodeName", "type" }],
    ///   "results": null | {
    ///     "field_name": {
    ///       "idColumn": { "name": "ID", "type": "STRING", "values": [...] },
    ///       "valuesColumn": { "name": "VALUE", "type": "DOUBLE"|..., "values": [...] },
    ///       "secondaryColumns": [{ "name": "DATE"|"CURRENCY", "values": [...] }],
    ///       "responseExceptions": [],
    ///       "partialErrorMap": { "errorIterator": null | [...] }
    ///     }
    ///   }
    /// }
    /// ```
    fn parse_bql_json(&self, json_str: &str) -> Result<RecordBatch, BlpError> {
        if json_str.len() <= BQL_TYPED_JSON_MAX_BYTES {
            self.parse_bql_json_typed(json_str)
        } else {
            self.parse_bql_json_value(json_str)
        }
    }

    fn parse_bql_json_typed(&self, json_str: &str) -> Result<RecordBatch, BlpError> {
        let response: BqlJsonResponse<'_> =
            serde_json::from_str(json_str).map_err(|e| BlpError::Internal {
                detail: format!("Failed to parse BQL JSON: {}", e),
            })?;
        materialize::from_typed(&response)
    }

    fn parse_bql_json_value(&self, json_str: &str) -> Result<RecordBatch, BlpError> {
        let response: JsonValue =
            serde_json::from_str(json_str).map_err(|e| BlpError::Internal {
                detail: format!("Failed to parse BQL JSON: {}", e),
            })?;
        materialize::from_value(&response)
    }

    /// Parse a cached/generated BQL JSON payload for benchmark-only replay.
    ///
    /// This is intentionally hidden behind `bench-internals` so production builds
    /// do not expose benchmark hooks or carry profiling behavior in public APIs.
    #[cfg(feature = "bench-internals")]
    pub fn parse_bql_json_for_bench(&self, json_str: &str) -> Result<RecordBatch, BlpError> {
        self.parse_bql_json(json_str)
    }

    /// Extract results from a BQL results element (legacy Element-API fallback).
    /// Note: secondaryColumns (DATE, CURRENCY) are only available in the JSON
    /// path — this path does not support them.
    fn extract_results(&mut self, results: &xbbg_core::Element) {
        xbbg_log::warn!(
            "BQL response routed to Element-API path — secondaryColumns will be missing"
        );
        let n = results.len();
        for i in 0..n {
            if let Some(row) = results.get_element(i) {
                // Each result row - extract all fields
                let num_children = row.num_children();
                for j in 0..num_children {
                    if let Some(child) = row.get_at(j) {
                        let name_str = child.name_str();
                        if let Some(value) = child.get_value(0) {
                            self.columns.append(name_str, value);
                        } else {
                            self.columns.append_null(name_str);
                        }
                    }
                }
                self.columns.end_row();
            }
        }
    }

    /// Flatten an element into path-value pairs (fallback for complex structures).
    fn flatten_element(&mut self, path: &str, element: &xbbg_core::Element) {
        let datatype = element.datatype();

        // For complex types, recurse into children
        if datatype.is_complex() {
            // If it's an array/sequence with values
            if element.is_array() {
                let n = element.len();
                for i in 0..n {
                    if let Some(child) = element.get_element(i) {
                        let child_path = if path.is_empty() {
                            format!("[{i}]")
                        } else {
                            format!("{path}[{i}]")
                        };
                        self.flatten_element(&child_path, &child);
                    }
                }
            } else {
                // Iterate named children
                let n = element.num_children();
                for i in 0..n {
                    if let Some(child) = element.get_at(i) {
                        let name = child.name_str();
                        let child_path = if path.is_empty() {
                            name.to_string()
                        } else {
                            format!("{}.{}", path, name)
                        };
                        self.flatten_element(&child_path, &child);
                    }
                }
            }
        } else {
            // Leaf value - add to columns
            if let Some(value) = element.get_value(0) {
                self.columns.append_str("path", path);

                // Convert value to string for generic representation
                let value_str = match &value {
                    xbbg_core::Value::String(s) | xbbg_core::Value::Enum(s) => s.to_string(),
                    xbbg_core::Value::Float64(f) => f.to_string(),
                    xbbg_core::Value::Int64(i) => i.to_string(),
                    xbbg_core::Value::Int32(i) => i.to_string(),
                    xbbg_core::Value::Bool(b) => b.to_string(),
                    xbbg_core::Value::Null => String::new(),
                    _ => format!("{:?}", value),
                };
                self.columns.append_str("value", &value_str);
                self.columns.end_row();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Float64Array, StringArray};
    use arrow_schema::DataType;

    fn make_state() -> BqlState {
        let (tx, _rx) = oneshot::channel();
        BqlState::new(tx)
    }

    fn parse_both_json_routes(json: &str) -> [Result<RecordBatch, BlpError>; 2] {
        assert!(json.len() <= BQL_TYPED_JSON_MAX_BYTES);
        let large_json = format!("{json}{}", " ".repeat(BQL_TYPED_JSON_MAX_BYTES));
        let state = make_state();
        [
            state.parse_bql_json(json),
            state.parse_bql_json(&large_json),
        ]
    }

    fn matching_json_routes(json: &str) -> RecordBatch {
        let [typed, value] = parse_both_json_routes(json);
        let typed = typed.expect("typed route parses");
        let value = value.expect("value route parses");
        assert_eq!(typed.schema(), value.schema());
        assert_eq!(typed.num_rows(), value.num_rows());
        for (typed_column, value_column) in typed.columns().iter().zip(value.columns()) {
            assert_eq!(typed_column.to_data(), value_column.to_data());
        }
        typed
    }

    #[test]
    fn parse_bql_json_structure_matches_across_routes() {
        let batch = matching_json_routes(
            r#"{
                "responseExceptions": [{"message": "partial response"}],
                "results": {
                    "z_last": {
                        "idColumn": {"values": ["not selected"]},
                        "valuesColumn": {"type": "INT32", "values": [3, 4, 5, 6]}
                    },
                    "b_price": {
                        "idColumn": {"values": ["X", null, 123]},
                        "valuesColumn": {"type": "double", "values": ["2.5", null]},
                        "secondaryColumns": [
                            {"name": "date", "values": ["not selected"]},
                            {"name": "A_EMPTY", "values": ["not selected"]},
                            {"values": ["unnamed"]}
                        ],
                        "responseExceptions": [{"message": "partial field", "nodeName": "price"}]
                    },
                    "a_empty": {
                        "idColumn": {"values": []},
                        "valuesColumn": {"values": [1]},
                        "secondaryColumns": [
                            {"name": "DATE", "type": "dAtE", "values": ["first"]}
                        ]
                    }
                }
            }"#,
        );
        let schema = batch.schema();
        let names: Vec<&str> = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(names, ["ticker", "date", "a_empty", "b_price", "z_last"]);
        assert_eq!(batch.num_rows(), 3);

        let tickers = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            tickers.iter().collect::<Vec<_>>(),
            [Some("X"), Some(""), Some("123")]
        );
        let dates = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            dates.iter().collect::<Vec<_>>(),
            [Some("first"), None, None]
        );
        for (index, expected) in [
            (2, [Some(1.0), None, None]),
            (3, [Some(2.5), None, None]),
            (4, [Some(3.0), Some(4.0), Some(5.0)]),
        ] {
            let values = batch
                .column(index)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(values.iter().collect::<Vec<_>>(), expected);
        }
    }

    #[test]
    fn parse_bql_json_missing_secondary_values_match_across_routes() {
        let batch = matching_json_routes(
            r#"{
                "results": {
                    "field_a": {
                        "idColumn": {"values": ["X", "Y"]},
                        "valuesColumn": {"values": [1, 2]},
                        "secondaryColumns": [{"name": "DATE", "type": "DATE"}]
                    },
                    "field_b": {
                        "secondaryColumns": [
                            {"name": "date", "values": ["not selected", "not selected"]}
                        ]
                    }
                }
            }"#,
        );
        let schema = batch.schema();
        let names: Vec<&str> = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(names, ["ticker", "date", "field_a", "field_b"]);
        // A declared dimension with omitted values follows the typed default:
        // emit nulls, and do not replace it with a later field's dimension.
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(batch.column(1).null_count(), 2);
        assert_eq!(schema.field(3).data_type(), &DataType::Float64);
        assert_eq!(batch.column(3).null_count(), 2);
    }

    #[test]
    fn parse_bql_json_inference_and_padding_match_across_routes() {
        let batch = matching_json_routes(
            r#"{
                "results": {
                    "all_null": {
                        "idColumn": {"values": ["A", "B", "C", "D", "E"]},
                        "valuesColumn": {"values": [null, null]}
                    },
                    "hinted_numeric": {
                        "valuesColumn": {"type": "iNtEgEr", "values": ["3.5", false, null, "bad"]}
                    },
                    "numeric": {
                        "valuesColumn": {
                            "values": [1, null, 2.5, 18446744073709551615, -0.0, "outside row count"]
                        }
                    },
                    "promoted": {
                        "valuesColumn": {
                            "values": [9007199254740993, 18446744073709551615, null, true, {"kind": "object"}]
                        }
                    },
                    "unknown": {
                        "valuesColumn": {"type": "BOOLEAN", "values": [false, null, "6"]}
                    }
                }
            }"#,
        );
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 6);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Float64);
        assert_eq!(batch.column(1).null_count(), 5);

        let hinted = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            hinted.iter().collect::<Vec<_>>(),
            [Some(3.5), None, None, None, None]
        );
        let numeric = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            numeric.iter().collect::<Vec<_>>(),
            [
                Some(1.0),
                None,
                Some(2.5),
                Some(u64::MAX as f64),
                Some(-0.0)
            ]
        );
        assert!(numeric.value(4).is_sign_negative());
        let promoted = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            promoted.iter().collect::<Vec<_>>(),
            [
                Some("9007199254740993"),
                Some("18446744073709551615"),
                None,
                Some("true"),
                Some(r#"{"kind":"object"}"#),
            ]
        );
        let unknown = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            unknown.iter().collect::<Vec<_>>(),
            [Some("false"), None, Some("6"), None, None]
        );
    }

    #[test]
    fn parse_bql_json_empty_results_and_ids_match_across_routes() {
        for json in [
            "{}",
            r#"{"results": null}"#,
            r#"{"results": {}, "responseExceptions": [{"nodeName": "no message"}]}"#,
        ] {
            let batch = matching_json_routes(json);
            assert_eq!(batch.num_rows(), 0);
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema().field(0).name(), "ticker");
            assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        }

        let batch = matching_json_routes(
            r#"{
                "results": {
                    "field": {
                        "valuesColumn": {"values": ["outside row count"]},
                        "secondaryColumns": [{"name": "DATE", "type": "DATE", "values": ["ignored"]}]
                    }
                }
            }"#,
        );
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Utf8);
        assert_eq!(batch.schema().field(2).data_type(), &DataType::Float64);
    }

    #[test]
    fn parse_bql_json_exception_joining_matches_across_routes() {
        for results in ["", r#", "results": null"#, r#", "results": {}"#] {
            for (exceptions, expected) in [
                (r#"[{"nodeName": "ignored"}, {"message": ""}]"#, ""),
                (
                    r#"[{"message": ""}, {"message": "bad query", "nodeName": "get(px)"}, {"message": "invalid field"}]"#,
                    "; bad query (in get(px)); invalid field",
                ),
            ] {
                let json = format!(
                    r#"{{"clientContext": {{"clientRequestId": "multiple-errors"}}, "responseExceptions": {exceptions}{results}}}"#
                );
                for result in parse_both_json_routes(&json) {
                    match result.expect_err("response exceptions must fail") {
                        BlpError::RequestFailure {
                            request_id, source, ..
                        } => {
                            assert_eq!(request_id.as_deref(), Some("multiple-errors"));
                            assert_eq!(
                                source.as_ref().map(ToString::to_string).as_deref(),
                                Some(expected)
                            );
                        }
                        other => panic!("unexpected error: {other:?}"),
                    }
                }
            }
        }
    }

    #[test]
    fn bql_cell_borrows_unescaped_strings_and_owns_escaped_strings() {
        let unescaped_json = r#""borrowed""#;
        let unescaped: BqlCell<'_> =
            serde_json::from_str(unescaped_json).expect("unescaped cell parses");
        match unescaped {
            BqlCell::String(Cow::Borrowed(value)) => assert_eq!(value, "borrowed"),
            other => panic!("expected borrowed string, got {other:?}"),
        }

        let escaped_json = r#""owned\nvalue""#;
        let escaped: BqlCell<'_> = serde_json::from_str(escaped_json).expect("escaped cell parses");
        match escaped {
            BqlCell::String(Cow::Owned(value)) => assert_eq!(value, "owned\nvalue"),
            other => panic!("expected owned string, got {other:?}"),
        }
    }

    fn mixed_cells_json(padding_len: usize) -> String {
        let padding = "x".repeat(padding_len);
        format!(
            r#"{{
                "padding": "{padding}",
                "results": {{
                    "mixed": {{
                        "idColumn": {{
                            "values": [
                                "plain",
                                "escaped\nvalue",
                                12.5,
                                true,
                                null,
                                {{"kind": "object", "value": 1}},
                                ["array", 2]
                            ]
                        }},
                        "valuesColumn": {{
                            "type": "STRING",
                            "values": [
                                "plain",
                                "escaped\nvalue",
                                12.5,
                                true,
                                null,
                                {{"kind": "object", "value": 1}},
                                ["array", 2]
                            ]
                        }}
                    }}
                }}
            }}"#
        )
    }

    #[test]
    fn parse_bql_json_cell_kinds_match_across_typed_and_value_routes() {
        let typed_json = mixed_cells_json(0);
        let value_json = mixed_cells_json(BQL_TYPED_JSON_MAX_BYTES);
        assert!(typed_json.len() <= BQL_TYPED_JSON_MAX_BYTES);
        assert!(value_json.len() > BQL_TYPED_JSON_MAX_BYTES);

        let typed = make_state()
            .parse_bql_json(&typed_json)
            .expect("typed route parses");
        let value = make_state()
            .parse_bql_json(&value_json)
            .expect("value route parses");
        assert_eq!(typed.schema(), value.schema());

        let typed_tickers = typed
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("typed tickers are utf8");
        let value_tickers = value
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("value tickers are utf8");
        let expected_tickers = vec![
            Some("plain"),
            Some("escaped\nvalue"),
            Some("12.5"),
            Some("true"),
            Some(""),
            Some(r#"{"kind":"object","value":1}"#),
            Some(r#"["array",2]"#),
        ];
        assert_eq!(typed_tickers.iter().collect::<Vec<_>>(), expected_tickers);
        assert_eq!(value_tickers.iter().collect::<Vec<_>>(), expected_tickers);

        let typed_values = typed
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("typed mixed values are utf8");
        let value_values = value
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("value mixed values are utf8");
        let expected_values = vec![
            Some("plain"),
            Some("escaped\nvalue"),
            Some("12.5"),
            Some("true"),
            None,
            Some(r#"{"kind":"object","value":1}"#),
            Some(r#"["array",2]"#),
        ];
        assert_eq!(typed_values.iter().collect::<Vec<_>>(), expected_values);
        assert_eq!(value_values.iter().collect::<Vec<_>>(), expected_values);
    }

    fn boundary_numbers_json(padding_len: usize) -> String {
        let padding = "x".repeat(padding_len);
        format!(
            r#"{{
                "padding": "{padding}",
                "results": {{
                    "numeric_values": {{
                        "idColumn": {{
                            "values": [
                                9007199254740993,
                                18446744073709551615,
                                -9223372036854775808,
                                -0.0,
                                1e-6
                            ]
                        }},
                        "valuesColumn": {{
                            "type": "DOUBLE",
                            "values": [
                                9007199254740993,
                                18446744073709551615,
                                -9223372036854775808,
                                -0.0,
                                1e-6
                            ]
                        }}
                    }},
                    "text_values": {{
                        "idColumn": {{
                            "values": [
                                9007199254740993,
                                18446744073709551615,
                                -9223372036854775808,
                                -0.0,
                                1e-6
                            ]
                        }},
                        "valuesColumn": {{
                            "type": "STRING",
                            "values": [
                                9007199254740993,
                                18446744073709551615,
                                -9223372036854775808,
                                -0.0,
                                1e-6
                            ]
                        }}
                    }}
                }}
            }}"#
        )
    }

    #[test]
    fn parse_bql_json_numbers_match_across_typed_and_value_routes() {
        let typed_json = boundary_numbers_json(0);
        let value_json = boundary_numbers_json(BQL_TYPED_JSON_MAX_BYTES);
        assert!(typed_json.len() <= BQL_TYPED_JSON_MAX_BYTES);
        assert!(value_json.len() > BQL_TYPED_JSON_MAX_BYTES);

        let typed = make_state()
            .parse_bql_json(&typed_json)
            .expect("typed route parses");
        let value = make_state()
            .parse_bql_json(&value_json)
            .expect("value route parses");
        assert_eq!(typed.schema(), value.schema());

        let expected_text = vec![
            Some("9007199254740993"),
            Some("18446744073709551615"),
            Some("-9223372036854775808"),
            Some("-0"),
            Some("0.000001"),
        ];
        for column_index in [0, 2] {
            let typed_text = typed
                .column(column_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("typed number text is utf8");
            let value_text = value
                .column(column_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("value number text is utf8");
            assert_eq!(typed_text.iter().collect::<Vec<_>>(), expected_text);
            assert_eq!(value_text.iter().collect::<Vec<_>>(), expected_text);
        }

        let typed_numeric = typed
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("typed numeric values are f64");
        let value_numeric = value
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value numeric values are f64");
        let expected_numeric = vec![
            Some(9007199254740993_u64 as f64),
            Some(u64::MAX as f64),
            Some(i64::MIN as f64),
            Some(-0.0),
            Some(1e-6),
        ];
        assert_eq!(typed_numeric.iter().collect::<Vec<_>>(), expected_numeric);
        assert_eq!(value_numeric.iter().collect::<Vec<_>>(), expected_numeric);
        assert!(typed_numeric.value(3).is_sign_negative());
        assert!(value_numeric.value(3).is_sign_negative());
    }

    fn assert_failed_response_diagnostics(json: &str) {
        let error = make_state()
            .parse_bql_json(json)
            .expect_err("response exception must fail");
        match error {
            BlpError::RequestFailure {
                service,
                operation,
                cid,
                label,
                request_id,
                source,
            } => {
                assert_eq!(service, "//blp/bqlsvc");
                assert_eq!(operation.as_deref(), Some("sendQuery"));
                assert!(cid.is_none());
                assert!(label.is_none());
                assert_eq!(request_id.as_deref(), Some("failed-request"));
                assert_eq!(
                    source.as_ref().map(ToString::to_string).as_deref(),
                    Some("bad query (in get(px))")
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parse_bql_json_failure_diagnostics_match_across_routes() {
        let make_json = |padding_len| {
            let padding = "x".repeat(padding_len);
            format!(
                r#"{{
                    "padding": "{padding}",
                    "clientContext": {{ "clientRequestId": "failed-request" }},
                    "responseExceptions": [
                        {{ "message": "bad query", "nodeName": "get(px)" }}
                    ],
                    "results": null
                }}"#
            )
        };
        let typed_json = make_json(0);
        let value_json = make_json(BQL_TYPED_JSON_MAX_BYTES);
        assert!(typed_json.len() <= BQL_TYPED_JSON_MAX_BYTES);
        assert!(value_json.len() > BQL_TYPED_JSON_MAX_BYTES);

        assert_failed_response_diagnostics(&typed_json);
        assert_failed_response_diagnostics(&value_json);
    }

    #[test]
    fn parse_bql_json_extracts_secondary_columns() {
        let json = r#"{
            "clientContext": { "clientRequestId": "abc" },
            "responseExceptions": null,
            "results": {
                "px_last": {
                    "idColumn": {
                        "name": "ID",
                        "type": "STRING",
                        "values": ["AAPL US Equity", "AAPL US Equity", "AAPL US Equity"]
                    },
                    "valuesColumn": {
                        "name": "VALUE",
                        "type": "DOUBLE",
                        "values": [150.1, 151.2, 152.3]
                    },
                    "secondaryColumns": [
                        {
                            "name": "DATE",
                            "type": "DATE",
                            "values": ["2026-04-10", "2026-04-11", "2026-04-14"]
                        },
                        {
                            "name": "CURRENCY",
                            "type": "STRING",
                            "values": ["USD", "USD", "USD"]
                        }
                    ],
                    "responseExceptions": [],
                    "partialErrorMap": { "errorIterator": null }
                }
            }
        }"#;

        let batch = make_state().parse_bql_json(json).expect("parse ok");
        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["ticker", "date", "currency", "px_last"]);
        assert_eq!(batch.num_rows(), 3);

        let dates = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("date column is utf8");
        assert_eq!(dates.value(0), "2026-04-10");
        assert_eq!(dates.value(2), "2026-04-14");

        let px = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("px_last column is f64");
        assert_eq!(px.value(0), 150.1);
    }

    #[test]
    fn parse_bql_json_dedupes_secondary_columns_across_fields() {
        let json = r#"{
            "results": {
                "px_last": {
                    "idColumn": { "values": ["T"] },
                    "valuesColumn": { "values": [1.0] },
                    "secondaryColumns": [
                        { "name": "DATE", "values": ["2026-04-10"] }
                    ]
                },
                "px_open": {
                    "idColumn": { "values": ["T"] },
                    "valuesColumn": { "values": [0.9] },
                    "secondaryColumns": [
                        { "name": "DATE", "values": ["2026-04-10"] }
                    ]
                }
            }
        }"#;

        let batch = make_state().parse_bql_json(json).expect("parse ok");
        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        // DATE should appear exactly once, primary fields follow in insertion order
        let date_count = names.iter().filter(|n| **n == "date").count();
        assert_eq!(date_count, 1);
        assert!(names.contains(&"px_last"));
        assert!(names.contains(&"px_open"));
    }

    #[test]
    fn parse_bql_json_mismatched_field_lengths_truncates() {
        let json = r#"{
            "results": {
                "field_a": {
                    "idColumn": { "values": ["X", "Y"] },
                    "valuesColumn": { "type": "DOUBLE", "values": [1.0, 2.0] }
                },
                "field_b": {
                    "idColumn": { "values": ["X", "Y", "Z", "W"] },
                    "valuesColumn": { "type": "DOUBLE", "values": [10.0, 20.0, 30.0, 40.0] }
                }
            }
        }"#;

        let batch = make_state().parse_bql_json(json).expect("parse ok");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
        let col_b = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("field_b is f64");
        assert_eq!(col_b.value(0), 10.0);
        assert_eq!(col_b.value(1), 20.0);
    }

    #[test]
    fn parse_bql_json_uses_type_hint_over_value_sniffing() {
        let json = r#"{
            "results": {
                "sector": {
                    "idColumn": { "values": ["AAPL"] },
                    "valuesColumn": { "type": "STRING", "values": ["Technology"] }
                }
            }
        }"#;

        let batch = make_state().parse_bql_json(json).expect("parse ok");
        let col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("sector is utf8 via type hint");
        assert_eq!(col.value(0), "Technology");
    }

    fn large_bql_json_with_padding() -> String {
        let padding = "x".repeat(BQL_TYPED_JSON_MAX_BYTES);
        format!(
            r#"{{
            "clientContext": {{ "clientRequestId": "large-request" }},
            "responseExceptions": [{{ "message": "partial top-level warning", "nodeName": "query" }}],
            "padding": "{padding}",
            "results": {{
                "px_last": {{
                    "idColumn": {{
                        "name": "ID",
                        "type": "STRING",
                        "values": ["AAPL US Equity", "MSFT US Equity", "IBM US Equity"]
                    }},
                    "valuesColumn": {{
                        "name": "VALUE",
                        "type": "DOUBLE",
                        "values": [150.1, null, "bad"]
                    }},
                    "secondaryColumns": [
                        {{
                            "name": "DATE",
                            "type": "DATE",
                            "values": ["2026-04-10", "2026-04-11", "2026-04-12"]
                        }},
                        {{
                            "name": "ASOF",
                            "type": "DATETIME",
                            "values": ["2026-04-10T12:30:00", "2026-04-11T12:30:00", "2026-04-12T12:30:00"]
                        }},
                        {{
                            "name": "CONFIDENCE",
                            "type": "STRING",
                            "values": ["0.95", null, "0.75"]
                        }}
                    ],
                    "responseExceptions": [{{ "message": "field warning", "nodeName": "px_last" }}]
                }},
                "rating": {{
                    "idColumn": {{ "type": "STRING", "values": ["AAPL US Equity", "MSFT US Equity", "IBM US Equity"] }},
                    "valuesColumn": {{ "type": "STRING", "values": ["1", "2", "3"] }},
                    "secondaryColumns": []
                }},
                "volume": {{
                    "idColumn": {{ "type": "STRING", "values": ["AAPL US Equity", "MSFT US Equity", "IBM US Equity", "TSLA US Equity"] }},
                    "valuesColumn": {{ "type": "INT64", "values": [1000, 2000, 3000, 4000] }},
                    "secondaryColumns": [
                        {{ "name": "DATE", "type": "DATE", "values": ["2026-04-10", "2026-04-11", "2026-04-12", "2026-04-13"] }}
                    ]
                }}
            }}
        }}"#
        )
    }

    #[test]
    fn parse_bql_json_large_payload_value_path_preserves_schema_and_values() {
        let json = large_bql_json_with_padding();
        assert!(json.len() > BQL_TYPED_JSON_MAX_BYTES);

        let batch = make_state().parse_bql_json(&json).expect("parse ok");
        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "ticker",
                "date",
                "asof",
                "confidence",
                "px_last",
                "rating",
                "volume",
            ]
        );
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 7);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(4).data_type(), &DataType::Float64);
        assert_eq!(schema.field(5).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(6).data_type(), &DataType::Float64);

        let ticker = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("ticker is utf8");
        assert_eq!(ticker.value(0), "AAPL US Equity");
        assert_eq!(ticker.value(2), "IBM US Equity");

        let dates = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("date is utf8");
        assert_eq!(dates.value(0), "2026-04-10");
        assert_eq!(dates.value(2), "2026-04-12");

        let asof = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("datetime is utf8");
        assert_eq!(asof.value(1), "2026-04-11T12:30:00");

        let confidence = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("confidence is utf8");
        assert_eq!(confidence.value(0), "0.95");
        assert!(confidence.is_null(1));

        let px_last = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("px_last is f64");
        assert_eq!(px_last.value(0), 150.1);
        assert!(px_last.is_null(1));
        assert!(px_last.is_null(2));

        let rating = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("rating remains utf8 via type hint");
        assert_eq!(rating.value(0), "1");
        assert_eq!(rating.value(2), "3");

        let volume = batch
            .column(6)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("volume is f64 via type hint");
        assert_eq!(volume.value(0), 1000.0);
        assert_eq!(volume.value(2), 3000.0);
    }

    #[test]
    fn parse_bql_json_keeps_date_and_datetime_as_utf8_for_compatibility() {
        let json = r#"{
            "results": {
                "event_count": {
                    "idColumn": { "type": "STRING", "values": ["AAPL US Equity"] },
                    "valuesColumn": { "type": "INT32", "values": [2] },
                    "secondaryColumns": [
                        { "name": "DATE", "type": "DATE", "values": ["2026-04-10"] },
                        { "name": "EVENT_TIME", "type": "DATETIME", "values": ["2026-04-10T09:30:00"] }
                    ]
                }
            }
        }"#;

        let batch = make_state().parse_bql_json(json).expect("parse ok");
        let schema = batch.schema();
        assert_eq!(schema.field(1).name(), "date");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).name(), "event_time");
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);

        let date = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("date remains utf8");
        let event_time = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("datetime remains utf8");
        assert_eq!(date.value(0), "2026-04-10");
        assert_eq!(event_time.value(0), "2026-04-10T09:30:00");
    }
}
