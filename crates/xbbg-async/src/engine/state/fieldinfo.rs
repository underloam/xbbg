//! FieldInfo extractor state for //blp/apiflds FieldInfoRequest responses.
//!
//! Produces a clean table with columns:
//! - field: Field mnemonic (e.g., "PX_LAST")
//! - type: Arrow type string (e.g., "float64", "string", "date32")
//! - description: Field description
//! - category: Category name
//!
//! Extracts directly from Bloomberg Elements without JSON intermediate.

use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use tokio::sync::oneshot;
use xbbg_log::trace;

use super::value_utils::top_level_response_error;
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
    ///     fieldInfo {
    ///       id: "PX_LAST"
    ///       mnemonic: "PX_LAST"
    ///       description: "Last Price"
    ///       datatype: "Double"
    ///       ftype: "Price"
    ///       categoryName[]: ["Analysis", "Pricing"]
    ///     }
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

            // Get fieldInfo sub-element
            let Some(field_info) = item.get_by_str("fieldInfo") else {
                continue;
            };

            // Get field mnemonic (prefer mnemonic over id)
            let field = field_info
                .get_by_str("mnemonic")
                .and_then(|e| e.get_str(0))
                .or_else(|| field_info.get_by_str("id").and_then(|e| e.get_str(0)))
                .unwrap_or("");

            if field.is_empty() {
                continue;
            }

            // apiflds uses Datetime for Date, Time and full timestamps; ftype
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
        }
    }

    /// Build the final RecordBatch.
    fn build_batch(&mut self) -> Result<RecordBatch, BlpError> {
        let field_array = self.field_builder.finish();
        let type_array = self.type_builder.finish();
        let description_array = self.description_builder.finish();
        let category_array = self.category_builder.finish();

        let schema = Arc::new(Schema::new(vec![
            Field::new("field", DataType::Utf8, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("category", DataType::Utf8, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(field_array),
                Arc::new(type_array),
                Arc::new(description_array),
                Arc::new(category_array),
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
    use crate::field_cache::FieldTypeResolver;
    use arrow_array::StringArray;
    use xbbg_core::test_support::TestEvent;
    use xbbg_core::EventType;

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
                {"fieldInfo":{"mnemonic":"SYNTHETIC_FALLBACK","ftype":"Time"}}
            ]}"#);
            },
        );
        let (sender, mut receiver) = oneshot::channel();
        FieldInfoState::new(sender).finish(&event.event().messages().next().unwrap());
        let batch = receiver.try_recv().unwrap().unwrap();
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
                Some("time64")
            ]
        );
        let dir = tempfile::tempdir().unwrap();
        let resolver = FieldTypeResolver::with_cache_path(dir.path().join("field_cache.json"));
        resolver.insert_from_response(&batch);
        assert_eq!(
            resolver.get_arrow_type("SYNTHETIC_TIME").as_deref(),
            Some("time64")
        );
        assert_eq!(
            resolver.get_arrow_type("SYNTHETIC_DATE").as_deref(),
            Some("date32")
        );
        assert_eq!(
            resolver.get_arrow_type("SYNTHETIC_STAMP").as_deref(),
            Some("timestamp")
        );
    }
}
