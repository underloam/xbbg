//! FieldInfo extractor state for //blp/apiflds FieldInfoRequest responses.
//!
//! Produces a clean table with columns:
//! - field: Field mnemonic (e.g., "PX_LAST")
//! - type: Arrow type string (e.g., "float64", "string", "date32")
//! - description: Field description
//! - category: Category name
//! - id: Echoed request id (falls back to the mnemonic when absent)
//!
//! Extracts directly from Bloomberg Elements without JSON intermediate.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::StringBuilder;
use arrow_schema::{DataType, Field, Schema};
use tokio::sync::oneshot;
use xbbg_log::trace;

use super::value_utils::top_level_response_error;
use crate::diagnostics::{FieldErrorMeta, METADATA_KEY_FIELD_ERRORS};
use crate::field_cache::BlpFieldType;
use xbbg_core::{BlpError, Message};

/// State for a FieldInfoRequest that extracts field metadata.
pub struct FieldInfoState {
    /// Field mnemonic builder
    field_builder: StringBuilder,
    /// Arrow type builder
    type_builder: StringBuilder,
    /// Description builder
    description_builder: StringBuilder,
    /// Category builder
    category_builder: StringBuilder,
    /// Enclosing echoed field id builder
    id_builder: StringBuilder,
    /// Field lookup refusals, retained across partial responses
    field_errors: BTreeMap<String, FieldErrorMeta>,
    /// Reply channel
    pub reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
}

impl FieldInfoState {
    /// Create a new FieldInfo state.
    pub fn new(reply: oneshot::Sender<Result<RecordBatch, BlpError>>) -> Self {
        Self {
            field_builder: StringBuilder::new(),
            type_builder: StringBuilder::new(),
            description_builder: StringBuilder::new(),
            category_builder: StringBuilder::new(),
            id_builder: StringBuilder::new(),
            field_errors: BTreeMap::new(),
            reply,
        }
    }

    /// Process a PARTIAL_RESPONSE message.
    pub fn on_partial(&mut self, msg: &Message) {
        self.process_message(msg);
    }

    /// Process the final RESPONSE message and send the result via reply channel.
    pub fn finish(mut self, msg: &Message) {
        if let Some(error) = top_level_response_error(msg, "//blp/apiflds", "FieldInfoRequest") {
            let _ = self.reply.send(Err(error));
            return;
        }

        self.process_message(msg);
        let result = self.build_batch();
        if let Ok(ref batch) = result {
            xbbg_log::debug!(rows = batch.num_rows(), "fieldinfo finish");
        }
        let _ = self.reply.send(result);
    }

    /// Process a message by extracting field info using Element API.
    ///
    /// Bloomberg structure:
    /// ```text
    /// FieldInfoResponse {
    ///   fieldData[] {
    ///     id: "pr000001"
    ///     fieldInfo {
    ///       mnemonic: "PX_LAST"
    ///       description: "Last Price"
    ///       datatype: "Double"
    ///       ftype: "Price"
    ///       categoryName[]: ["Analysis", "Pricing"]
    ///     }
    ///     fieldError? { source, code, category, subcategory, message }
    ///   }
    ///   fieldSearchError? { ... }
    /// }
    /// ```
    fn process_message(&mut self, msg: &Message) {
        let root = msg.elements();

        // Get fieldData array
        let Some(field_data) = root.get_by_str("fieldData") else {
            trace!("No fieldData in message");
            return;
        };

        let n = field_data.len();
        for i in 0..n {
            let Some(item) = field_data.get_element(i) else {
                continue;
            };

            let id = item.get_by_str("id").and_then(|element| element.get_str(0));
            if let Some(error) = item
                .get_by_str("fieldError")
                .filter(|element| !element.is_null())
            {
                let string = |name| {
                    error
                        .get_by_str(name)
                        .and_then(|element| element.get_str(0))
                        .unwrap_or("")
                        .to_string()
                };
                self.field_errors.insert(
                    id.unwrap_or("").to_string(),
                    FieldErrorMeta {
                        category: string("category"),
                        code: error
                            .get_by_str("code")
                            .and_then(|element| element.get_i32(0))
                            .unwrap_or_default(),
                        subcategory: string("subcategory"),
                        message: string("message"),
                        source: error
                            .get_by_str("source")
                            .and_then(|element| element.get_str(0))
                            .map(str::to_string),
                    },
                );
                continue;
            }

            let Some(field_info) = item
                .get_by_str("fieldInfo")
                .filter(|element| !element.is_null())
            else {
                continue;
            };

            // Keep the canonical mnemonic separate from the echoed request id.
            let field = field_info
                .get_by_str("mnemonic")
                .and_then(|e| e.get_str(0))
                .or_else(|| field_info.get_by_str("id").and_then(|e| e.get_str(0)))
                .or(id)
                .unwrap_or("");

            if field.is_empty() {
                continue;
            }

            // apiflds uses Datetime for temporal and textual fields; ftype
            // disambiguates those without overriding other concrete datatypes.
            let datatype = field_info.get_by_str("datatype");
            let ftype = field_info.get_by_str("ftype");
            let blp_type = BlpFieldType::from_metadata(
                datatype.as_ref().and_then(|element| element.get_str(0)),
                ftype.as_ref().and_then(|element| element.get_str(0)),
            );
            let arrow_type = blp_type.to_arrow_type_str();

            // Get description
            let description = field_info
                .get_by_str("description")
                .and_then(|e| e.get_str(0))
                .unwrap_or("");

            // Get category (first one if multiple)
            let category = field_info
                .get_by_str("categoryName")
                .and_then(|cats| cats.get_str(0))
                .unwrap_or("");

            self.field_builder.append_value(field);
            self.type_builder.append_value(arrow_type);
            self.description_builder.append_value(description);
            self.category_builder.append_value(category);
            self.id_builder.append_value(id.unwrap_or(field));
        }
    }

    /// Build the final RecordBatch.
    fn build_batch(&mut self) -> Result<RecordBatch, BlpError> {
        let field_array = self.field_builder.finish();
        let type_array = self.type_builder.finish();
        let description_array = self.description_builder.finish();
        let category_array = self.category_builder.finish();
        let id_array = self.id_builder.finish();
        let mut metadata = HashMap::new();
        if !self.field_errors.is_empty() {
            let errors =
                serde_json::to_string(&self.field_errors).map_err(|error| BlpError::Internal {
                    detail: format!("serialize FieldInfo errors: {error}"),
                })?;
            metadata.insert(METADATA_KEY_FIELD_ERRORS.to_string(), errors);
        }

        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("field", DataType::Utf8, false),
                Field::new("type", DataType::Utf8, false),
                Field::new("description", DataType::Utf8, true),
                Field::new("category", DataType::Utf8, true),
                Field::new("id", DataType::Utf8, false),
            ],
            metadata,
        ));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(field_array),
                Arc::new(type_array),
                Arc::new(description_array),
                Arc::new(category_array),
                Arc::new(id_array),
            ],
        )
        .map_err(|e| BlpError::Internal {
            detail: format!("build RecordBatch: {e}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::subscription_types::SubscriptionTypeResolver;
    use crate::field_cache::{FieldInfo, FieldTypeResolver};
    use arrow_array::StringArray;
    use xbbg_core::EventType;
    use xbbg_core::test_support::TestEvent;

    fn fieldinfo_response(event_type: EventType, rows: serde_json::Value) -> TestEvent {
        let schema = r#"<ServiceDefinition name="xbbg.test.fieldinfo.errors" version="1.0.0.0">
            <service name="//xbbg/test/fieldinfo" version="1.0.0.0">
                <event name="FieldInfoResponse" eventType="FieldInfoResponseType"/>
            </service>
            <schema>
                <sequenceType name="FieldInfoResponseType">
                    <element name="fieldData" type="FieldInfoDataType" minOccurs="0" maxOccurs="unbounded"/>
                </sequenceType>
                <sequenceType name="FieldInfoDataType">
                    <element name="id" type="String" minOccurs="0"/>
                    <element name="fieldInfo" type="FieldMetadataType" minOccurs="0"/>
                    <element name="fieldError" type="FieldErrorType" minOccurs="0"/>
                </sequenceType>
                <sequenceType name="FieldMetadataType">
                    <element name="mnemonic" type="String"/>
                    <element name="datatype" type="String" minOccurs="0"/>
                    <element name="ftype" type="String" minOccurs="0"/>
                    <element name="description" type="String" minOccurs="0"/>
                    <element name="categoryName" type="String" minOccurs="0" maxOccurs="unbounded"/>
                </sequenceType>
                <sequenceType name="FieldErrorType">
                    <element name="source" type="String" minOccurs="0"/>
                    <element name="code" type="Int32"/>
                    <element name="category" type="String"/>
                    <element name="subcategory" type="String" minOccurs="0"/>
                    <element name="message" type="String"/>
                </sequenceType>
            </schema>
        </ServiceDefinition>"#;
        TestEvent::with_schema(schema, event_type, "FieldInfoResponse", &[], |formatter| {
            formatter.json(&serde_json::json!({"fieldData": rows}).to_string());
        })
    }

    fn string_values<'a>(batch: &'a RecordBatch, name: &str) -> Vec<&'a str> {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(Option::unwrap)
            .collect()
    }

    fn capture_warnings(action: impl FnOnce()) -> String {
        #[derive(Clone)]
        struct Capture(Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Capture {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let logs = Arc::new(std::sync::Mutex::new(Vec::new()));
        let output = Arc::clone(&logs);
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .without_time()
            .with_ansi(false)
            .with_writer(move || Capture(Arc::clone(&output)))
            .finish();
        tracing::subscriber::with_default(subscriber, action);
        let bytes = logs.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn mixed_fieldinfo_response_preserves_echoed_ids_refusals_and_cache_aliases() {
        let event = fieldinfo_response(
            EventType::Response,
            serde_json::json!([
                {
                    "id": "pr000001",
                    "fieldInfo": {
                        "mnemonic": "PX_LAST",
                        "datatype": "Double",
                        "description": "Last Price",
                        "categoryName": ["Pricing"]
                    }
                },
                {
                    "id": "Not_A_Field",
                    "fieldError": {
                        "source": "apiflds@synthetic",
                        "code": -103,
                        "category": "BAD_FLD",
                        "subcategory": "INVALID_FIELD",
                        "message": "Unknown Field Id/Mnemonic"
                    }
                },
                {"fieldInfo": {"mnemonic": "BID", "datatype": "Double"}}
            ]),
        );
        let (sender, mut receiver) = oneshot::channel();
        FieldInfoState::new(sender).finish(&event.event().messages().next().unwrap());
        let batch = receiver.try_recv().unwrap().unwrap();

        assert_eq!(string_values(&batch, "field"), ["PX_LAST", "BID"]);
        assert_eq!(string_values(&batch, "id"), ["pr000001", "BID"]);
        assert_eq!(string_values(&batch, "type"), ["float64", "float64"]);
        assert_eq!(string_values(&batch, "description"), ["Last Price", ""]);
        assert_eq!(string_values(&batch, "category"), ["Pricing", ""]);
        let errors: serde_json::Value =
            serde_json::from_str(&batch.schema().metadata()[METADATA_KEY_FIELD_ERRORS]).unwrap();
        assert_eq!(
            errors,
            serde_json::json!({
                "Not_A_Field": {
                    "source": "apiflds@synthetic",
                    "code": -103,
                    "category": "BAD_FLD",
                    "subcategory": "INVALID_FIELD",
                    "message": "Unknown Field Id/Mnemonic"
                }
            })
        );

        let requested = ["PR000001", "px_last", "bid", "not_a_field", "ABSENT"].map(str::to_string);
        let validation = crate::engine::FieldValidation::from_response(&requested, &batch).unwrap();
        assert_eq!(validation.invalid_fields, ["not_a_field", "ABSENT"]);
        let detail = validation.detail();
        for expected in [
            "Not_A_Field",
            "Unknown Field Id/Mnemonic",
            "BAD_FLD",
            "INVALID_FIELD",
            "-103",
            "apiflds@synthetic",
        ] {
            assert!(detail.contains(expected), "{detail}");
        }

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fields.json");
        let cache = FieldTypeResolver::with_cache_path(path.clone());
        cache.insert_from_response(&batch);
        assert_eq!(cache.get_arrow_type("PR000001").as_deref(), Some("float64"));
        assert_eq!(cache.get_arrow_type("px_last").as_deref(), Some("float64"));
        assert_eq!(cache.get("pr000001").unwrap().description, "Last Price");
        assert!(cache.get("Not_A_Field").is_none());
        cache.save_to_disk().unwrap();
        let reloaded = FieldTypeResolver::with_cache_path(path);
        assert_eq!(
            reloaded.get_arrow_type("pr000001").as_deref(),
            Some("float64")
        );
        assert_eq!(
            reloaded.get_arrow_type("PX_LAST").as_deref(),
            Some("float64")
        );
    }

    #[test]
    fn all_rejected_fieldinfo_keeps_typed_empty_columns_and_partial_refusals() {
        let first = fieldinfo_response(
            EventType::PartialResponse,
            serde_json::json!([{
                "id": "UNKNOWN_FIELD",
                "fieldError": {
                    "source": "apiflds@synthetic",
                    "code": -103,
                    "category": "BAD_FLD",
                    "subcategory": "INVALID_FIELD",
                    "message": "Unknown Field Id/Mnemonic"
                }
            }]),
        );
        let last = fieldinfo_response(
            EventType::Response,
            serde_json::json!([{
                "id": "REFUSED_FIELD",
                "fieldError": {
                    "code": -4,
                    "category": "NO_AUTH",
                    "subcategory": "NOT_ENTITLED",
                    "message": "Field metadata access refused"
                }
            }]),
        );
        let (sender, mut receiver) = oneshot::channel();
        let mut state = FieldInfoState::new(sender);
        state.on_partial(&first.event().messages().next().unwrap());
        state.finish(&last.event().messages().next().unwrap());
        let batch = receiver.try_recv().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["field", "type", "description", "category", "id"]
        );
        for field in batch.schema().fields() {
            assert_eq!(field.data_type(), &DataType::Utf8);
        }
        let errors: serde_json::Value =
            serde_json::from_str(&batch.schema().metadata()[METADATA_KEY_FIELD_ERRORS]).unwrap();
        assert_eq!(
            errors,
            serde_json::json!({
                "UNKNOWN_FIELD": {
                    "source": "apiflds@synthetic",
                    "code": -103,
                    "category": "BAD_FLD",
                    "subcategory": "INVALID_FIELD",
                    "message": "Unknown Field Id/Mnemonic"
                },
                "REFUSED_FIELD": {
                    "code": -4,
                    "category": "NO_AUTH",
                    "subcategory": "NOT_ENTITLED",
                    "message": "Field metadata access refused"
                }
            })
        );
        let requested = ["unknown_field", "refused_field"].map(str::to_string);
        let validation = crate::engine::FieldValidation::from_response(&requested, &batch).unwrap();
        assert_eq!(validation.invalid_fields, requested);
        assert!(
            validation
                .detail()
                .contains("Field metadata access refused")
        );
        assert!(validation.detail().contains("Unknown Field Id/Mnemonic"));
    }

    #[test]
    fn partial_success_keeps_its_echoed_id_when_the_final_response_refuses_another_id() {
        let first = fieldinfo_response(
            EventType::PartialResponse,
            serde_json::json!([{
                "id": "synthetic_alias",
                "fieldInfo": {"mnemonic": "SYNTHETIC_TIME", "datatype": "Datetime", "ftype": "Time"}
            }]),
        );
        let last = fieldinfo_response(
            EventType::Response,
            serde_json::json!([{
                "id": "REFUSED_FIELD",
                "fieldError": {
                    "source": "apiflds@synthetic",
                    "code": -4,
                    "category": "NO_AUTH",
                    "subcategory": "NOT_ENTITLED",
                    "message": "Field metadata access refused"
                }
            }]),
        );
        let (sender, mut receiver) = oneshot::channel();
        let mut state = FieldInfoState::new(sender);
        state.on_partial(&first.event().messages().next().unwrap());
        state.finish(&last.event().messages().next().unwrap());
        let batch = receiver.try_recv().unwrap().unwrap();
        assert_eq!(string_values(&batch, "id"), ["synthetic_alias"]);
        assert_eq!(string_values(&batch, "field"), ["SYNTHETIC_TIME"]);
        assert_eq!(string_values(&batch, "type"), ["time64"]);
        let requested = ["SYNTHETIC_ALIAS", "synthetic_time", "refused_field"].map(str::to_string);
        let validation = crate::engine::FieldValidation::from_response(&requested, &batch).unwrap();
        assert_eq!(validation.invalid_fields, ["refused_field"]);
        assert!(validation.detail().contains("apiflds@synthetic"));
    }

    #[test]
    fn type_lookup_warns_with_refusal_details_without_losing_successes_or_defaults() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        for answered in [false, true] {
            let mut rows = vec![serde_json::json!({
                "id": "REFUSED_FIELD",
                "fieldError": {
                    "source": "apiflds@synthetic",
                    "code": -103,
                    "category": "BAD_FLD",
                    "subcategory": "INVALID_FIELD",
                    "message": "Unknown Field Id/Mnemonic"
                }
            })];
            if answered {
                rows.push(serde_json::json!({
                    "id": "synthetic_alias",
                    "fieldInfo": {"mnemonic": "SYNTHETIC_DATE", "datatype": "Date"}
                }));
            }
            let event = fieldinfo_response(EventType::Response, rows.into());
            let (sender, mut receiver) = oneshot::channel();
            FieldInfoState::new(sender).finish(&event.event().messages().next().unwrap());
            let batch = receiver.try_recv().unwrap().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let cache = Arc::new(FieldTypeResolver::with_cache_path(
                directory.path().join("fields.json"),
            ));
            cache.insert(FieldInfo {
                field_id: "SYNTHETIC_CACHED".to_string(),
                arrow_type: "time64".to_string(),
                description: String::new(),
                category: String::new(),
            });
            let resolver = SubscriptionTypeResolver::with_query(
                Arc::clone(&cache),
                runtime.handle().clone(),
                move |_| Box::pin(std::future::ready(Ok(batch.clone()))),
            );
            let fields =
                ["synthetic_cached", "synthetic_alias", "REFUSED_FIELD"].map(str::to_string);
            let warnings = capture_warnings(|| {
                let resolved = runtime
                    .block_on(resolver.resolve_types(&fields, None, "float64"))
                    .unwrap();
                assert_eq!(resolved["synthetic_cached"], "time64");
                assert_eq!(
                    resolved["synthetic_alias"],
                    if answered { "date32" } else { "float64" }
                );
                assert_eq!(resolved["REFUSED_FIELD"], "float64");
                let kinds = runtime.block_on(resolver.resolve(&fields));
                assert_eq!(
                    kinds["synthetic_cached"],
                    crate::engine::state::FieldKind::Time64Micros
                );
                assert_eq!(
                    kinds["synthetic_alias"],
                    if answered {
                        crate::engine::state::FieldKind::Date32
                    } else {
                        crate::engine::state::FieldKind::Unknown
                    }
                );
                assert_eq!(
                    kinds["REFUSED_FIELD"],
                    crate::engine::state::FieldKind::Unknown
                );
            });
            for expected in [
                "refused_count=1",
                "REFUSED_FIELD",
                "BAD_FLD",
                "INVALID_FIELD",
                "-103",
                "Unknown Field Id/Mnemonic",
                "apiflds@synthetic",
            ] {
                assert!(warnings.contains(expected), "{warnings}");
            }
            assert!(cache.get("REFUSED_FIELD").is_none());
            assert_eq!(cache.get("synthetic_alias").is_some(), answered);
            assert_eq!(cache.get("SYNTHETIC_DATE").is_some(), answered);
        }
    }

    #[test]
    fn apiflds_datetime_ftype_reaches_the_field_cache() {
        let schema = r#"<ServiceDefinition name="xbbg.test.fieldinfo" version="1.0.0.0">
            <service name="//xbbg/test/fieldinfo" version="1.0.0.0">
                <event name="FieldInfoResponse" eventType="FieldInfoResponseType"/>
            </service>
            <schema>
                <sequenceType name="FieldInfoResponseType">
                    <element name="fieldData" type="FieldInfoDataType" minOccurs="0" maxOccurs="unbounded"/>
                </sequenceType>
                <sequenceType name="FieldInfoDataType">
                    <element name="fieldInfo" type="FieldMetadataType"/>
                </sequenceType>
                <sequenceType name="FieldMetadataType">
                    <element name="mnemonic" type="String"/>
                    <element name="datatype" type="String" minOccurs="0"/>
                    <element name="ftype" type="String" minOccurs="0"/>
                </sequenceType>
            </schema>
        </ServiceDefinition>"#;
        let event = TestEvent::with_schema(
            schema,
            EventType::Response,
            "FieldInfoResponse",
            &[],
            |formatter| {
                formatter.json(r#"{"fieldData":[
                {"fieldInfo":{"mnemonic":"SYNTHETIC_TIME","datatype":"Datetime","ftype":"Time"}},
                {"fieldInfo":{"mnemonic":"SYNTHETIC_DATE","datatype":"Datetime","ftype":"Date"}},
                {"fieldInfo":{"mnemonic":"SYNTHETIC_STAMP","datatype":"Datetime","ftype":"Datetime"}},
                {"fieldInfo":{"mnemonic":"SYNTHETIC_MIXED","datatype":"Datetime","ftype":"DateOrTime"}},
                {"fieldInfo":{"mnemonic":"SYNTHETIC_PRICE","datatype":"Double","ftype":"Price"}},
                {"fieldInfo":{"mnemonic":"SYNTHETIC_FALLBACK","ftype":"Time"}},
                {"fieldInfo":{"mnemonic":"SYNTHETIC_CHARACTER","datatype":"Datetime","ftype":"Character"}},
                {"fieldInfo":{"mnemonic":"SYNTHETIC_STRING","datatype":"Datetime","ftype":"String"}},
                {"fieldInfo":{"mnemonic":"SYNTHETIC_CHARACTER_FALLBACK","ftype":"Character"}},
                {"fieldInfo":{"mnemonic":"SYNTHETIC_STRING_FALLBACK","ftype":"String"}},
                {"fieldInfo":{"mnemonic":"SYNTHETIC_CONCRETE_NUMBER","datatype":"Double","ftype":"Character"}},
                {"fieldInfo":{"mnemonic":"SYNTHETIC_CONCRETE_STRING","datatype":"String","ftype":"Datetime"}}
            ]}"#);
            },
        );
        let (sender, mut receiver) = oneshot::channel();
        FieldInfoState::new(sender).finish(&event.event().messages().next().unwrap());
        let batch = receiver.try_recv().unwrap().unwrap();
        assert!(
            !batch
                .schema()
                .metadata()
                .contains_key(METADATA_KEY_FIELD_ERRORS)
        );
        assert_eq!(string_values(&batch, "id"), string_values(&batch, "field"));
        let types = batch
            .column_by_name("type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            types.iter().collect::<Vec<_>>(),
            [
                Some("time64"),
                Some("date32"),
                Some("timestamp"),
                Some("string"),
                Some("float64"),
                Some("time64"),
                Some("string"),
                Some("string"),
                Some("string"),
                Some("string"),
                Some("float64"),
                Some("string")
            ]
        );
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("field_cache.json");
        let resolver = FieldTypeResolver::with_cache_path(path.clone());
        resolver.insert_from_response(&batch);
        let fields = batch
            .column_by_name("field")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert_eq!(
                resolver.get_arrow_type(fields.value(i)).as_deref(),
                Some(types.value(i)),
                "in-memory metadata type for {}",
                fields.value(i)
            );
        }
        resolver.save_to_disk().unwrap();
        let reloaded = FieldTypeResolver::with_cache_path(path);
        for i in 0..batch.num_rows() {
            assert_eq!(
                reloaded.get_arrow_type(fields.value(i)).as_deref(),
                Some(types.value(i)),
                "persisted metadata type for {}",
                fields.value(i)
            );
        }
    }
}
