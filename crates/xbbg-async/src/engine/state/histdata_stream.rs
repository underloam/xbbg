//! Streaming historical data (bdh) state with Arrow builders.
//!
//! Unlike HistDataState, this state yields chunks immediately via a channel
//! instead of accumulating all data until the final response.
//!
//! Extracts directly from Bloomberg Elements without JSON intermediate.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use super::histdata::HistDataResponse;
use super::refdata::{LongMode, OutputFormat};
use super::value_utils::top_level_response_error;
use xbbg_core::{BlpError, Message};

/// Nonblocking SDK-callback delivery with one terminal notification on overflow.
/// The sender is taken before scheduling that notification, so queued chunks are
/// followed by exactly one error and no later response can silently resume.
pub(super) struct ResponseStream {
    sender: Option<mpsc::Sender<Result<RecordBatch, BlpError>>>,
    runtime: Option<Handle>,
    operation: &'static str,
}

impl ResponseStream {
    pub(super) fn new(
        sender: mpsc::Sender<Result<RecordBatch, BlpError>>,
        operation: &'static str,
    ) -> Self {
        Self {
            sender: Some(sender),
            runtime: Handle::try_current().ok(),
            operation,
        }
    }

    pub(super) fn is_closed(&self) -> bool {
        self.sender.as_ref().is_none_or(mpsc::Sender::is_closed)
    }

    pub(super) fn send(&mut self, item: Result<RecordBatch, BlpError>) {
        let Some(sender) = &self.sender else {
            return;
        };
        let terminal = item.is_err();
        match sender.try_send(item) {
            Ok(()) => {
                if terminal {
                    self.sender = None;
                }
            }
            Err(TrySendError::Closed(_)) => self.sender = None,
            Err(TrySendError::Full(item)) => {
                let error = item.err().unwrap_or_else(|| BlpError::Internal {
                    detail: format!(
                        "{} stream channel is full; response terminated to avoid silent data loss",
                        self.operation
                    ),
                });
                let sender = self.sender.take().expect("full stream has a sender");
                if let Some(runtime) = &self.runtime {
                    drop(runtime.spawn(async move {
                        // A closed receiver has explicitly abandoned the response.
                        let _ = sender.send(Err(error)).await;
                    }));
                } else {
                    // Public constructors also work outside Tokio. Only this
                    // terminal path needs a thread, never the SDK dispatcher.
                    // spawn panics on OS failure rather than hiding the error.
                    drop(std::thread::spawn(move || {
                        let _ = sender.blocking_send(Err(error));
                    }));
                }
            }
        }
    }
}

/// Streaming state for a historical data request (bdh).
///
/// Chunks use the same wide columns and diagnostics as accumulated historical
/// responses. Hints fix even all-null field types; without hints, a field remains
/// string-typed until its first observed value determines its type. An overflowing
/// channel delivers accepted chunks followed by an error, with no subsequent chunks.
pub struct HistDataStreamState {
    response: HistDataResponse,
    stream: ResponseStream,
}

impl HistDataStreamState {
    /// Create a new streaming histdata state.
    pub fn new(fields: Vec<String>, stream: mpsc::Sender<Result<RecordBatch, BlpError>>) -> Self {
        Self::with_types(fields, None, stream)
    }

    /// Create a streaming wide response with optional field type overrides.
    /// Unhinted fields use their observed Bloomberg scalar types.
    pub fn with_types(
        fields: Vec<String>,
        field_types: Option<HashMap<String, String>>,
        stream: mpsc::Sender<Result<RecordBatch, BlpError>>,
    ) -> Self {
        Self {
            response: HistDataResponse::new(
                fields,
                OutputFormat::Wide,
                LongMode::String,
                field_types,
            ),
            stream: ResponseStream::new(stream, "HistoricalDataRequest"),
        }
    }

    /// Process a PARTIAL_RESPONSE message and yield a chunk without blocking the dispatcher.
    pub fn on_partial(&mut self, msg: &Message) {
        if self.stream.is_closed() {
            return;
        }
        if let Some(error) = top_level_response_error(msg, "//blp/refdata", "HistoricalDataRequest")
        {
            self.stream.send(Err(error));
            return;
        }
        let _ = self.response.process_message(msg);
        if !self.response.is_empty() {
            self.stream.send(self.response.finish_batch());
        }
    }

    /// Process the final RESPONSE message and close the stream.
    pub fn finish(mut self, msg: &Message) {
        self.on_partial(msg);
    }

    /// Fail the stream, preserving the terminal error even when the channel is full.
    pub fn fail(mut self, error: BlpError) {
        self.stream.send(Err(error));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::state::{HistDataState, LongMode, OutputFormat};
    use arrow_array::{Array, Int64Array, StringArray};
    use tokio::sync::oneshot;
    use xbbg_core::test_support::TestEvent;
    use xbbg_core::EventType;

    fn response(event_type: EventType, security: serde_json::Value) -> TestEvent {
        // TestUtil materializes declared fields even when JSON omits them.
        // Leave absent diagnostics out of the schema, just as a real response does.
        let mut diagnostics = String::new();
        for (field, declaration) in [
            (
                "eidData",
                r#"<element name="eidData" type="Int32" maxOccurs="unbounded"/>"#,
            ),
            (
                "securityError",
                r#"<element name="securityError" type="ErrorInfo"/>"#,
            ),
            (
                "fieldExceptions",
                r#"<element name="fieldExceptions" type="FieldException" maxOccurs="unbounded"/>"#,
            ),
        ] {
            if security.get(field).is_some() {
                diagnostics.push_str(declaration);
            }
        }
        let schema = format!(
            r#"<ServiceDefinition name="xbbg.test.history" version="1.0.0.0">
            <service name="//xbbg/test/history" version="1.0.0.0">
                <event name="HistoricalDataResponse" eventType="HistoryResponse"/>
            </service>
            <schema>
                <sequenceType name="HistoryResponse">
                    <element name="securityData" type="SecurityData"/>
                </sequenceType>
                <sequenceType name="SecurityData">
                    <element name="security" type="String"/>
                    <element name="fieldData" type="HistoryRow" minOccurs="0" maxOccurs="unbounded"/>
                    {diagnostics}
                </sequenceType>
                <sequenceType name="HistoryRow">
                    <element name="date" type="Date"/>
                    <element name="COUNT" type="Int64" minOccurs="0"/>
                    <element name="TEXT" type="String" minOccurs="0"/>
                </sequenceType>
                <sequenceType name="ErrorInfo">
                    <element name="category" type="String"/>
                    <element name="code" type="Int32"/>
                    <element name="subcategory" type="String"/>
                    <element name="message" type="String"/>
                </sequenceType>
                <sequenceType name="FieldException">
                    <element name="fieldId" type="String"/>
                    <element name="errorInfo" type="ErrorInfo"/>
                </sequenceType>
            </schema>
        </ServiceDefinition>"#
        );
        TestEvent::with_schema(
            &schema,
            event_type,
            "HistoricalDataResponse",
            &[],
            |formatter| formatter.json(&serde_json::json!({"securityData": security}).to_string()),
        )
    }

    fn data_response(event_type: EventType, count: i64) -> TestEvent {
        response(
            event_type,
            serde_json::json!({
                "security": "TEST Equity",
                "fieldData": [{"date": "1970-01-02", "COUNT": count, "TEXT": "sample"}]
            }),
        )
    }

    #[test]
    fn streaming_history_matches_wide_oneshot_types_and_nulls() {
        let fields = vec!["COUNT".into(), "TEXT".into(), "MISSING".into()];
        let hints = HashMap::from([
            ("COUNT".into(), "int64".into()),
            ("TEXT".into(), "string".into()),
            ("MISSING".into(), "bool".into()),
        ]);
        let (reply, mut result) = oneshot::channel();
        let mut oneshot = HistDataState::with_format(
            fields.clone(),
            OutputFormat::Wide,
            LongMode::String,
            Some(hints.clone()),
            reply,
        );
        let (sender, mut receiver) = mpsc::channel(2);
        let mut stream = HistDataStreamState::with_types(fields, Some(hints), sender);
        let first = data_response(EventType::PartialResponse, 9_007_199_254_740_993);
        let last = data_response(EventType::Response, 7);
        let mut messages = first.event().messages();
        let message = messages.next().unwrap();
        oneshot.on_partial(&message);
        stream.on_partial(&message);
        let mut messages = last.event().messages();
        let message = messages.next().unwrap();
        oneshot.finish(&message);
        stream.finish(&message);
        let expected = result.try_recv().unwrap().unwrap();
        let chunks = [
            receiver.try_recv().unwrap().unwrap(),
            receiver.try_recv().unwrap().unwrap(),
        ];
        for chunk in &chunks {
            assert_eq!(chunk.schema(), expected.schema());
            assert!(chunk.column_by_name("MISSING").unwrap().is_null(0));
        }
        let combined = arrow_select::concat::concat_batches(&expected.schema(), &chunks).unwrap();
        assert_eq!(combined, expected);
        assert_eq!(
            combined
                .column_by_name("COUNT")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            9_007_199_254_740_993
        );
        assert_eq!(
            combined
                .column_by_name("TEXT")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "sample"
        );
    }

    #[test]
    fn streaming_history_honors_overrides_and_retains_inferred_types_between_chunks() {
        for hints in [
            None,
            Some(HashMap::from([("COUNT".into(), "string".into())])),
        ] {
            let fields = vec!["COUNT".into(), "TEXT".into()];
            let (reply, mut result) = oneshot::channel();
            let mut oneshot = HistDataState::with_format(
                fields.clone(),
                OutputFormat::Wide,
                LongMode::String,
                hints.clone(),
                reply,
            );
            let (sender, mut receiver) = mpsc::channel(2);
            let mut stream = HistDataStreamState::with_types(fields, hints.clone(), sender);
            let first = data_response(EventType::PartialResponse, 42);
            let last = response(
                EventType::Response,
                serde_json::json!({
                    "security": "TEST Equity",
                    "fieldData": [{"date": "1970-01-03", "COUNT": null, "TEXT": null}]
                }),
            );
            let mut messages = first.event().messages();
            let message = messages.next().unwrap();
            oneshot.on_partial(&message);
            stream.on_partial(&message);
            let mut messages = last.event().messages();
            let message = messages.next().unwrap();
            oneshot.finish(&message);
            stream.finish(&message);
            let expected = result.try_recv().unwrap().unwrap();
            let chunks = [
                receiver.try_recv().unwrap().unwrap(),
                receiver.try_recv().unwrap().unwrap(),
            ];
            assert_eq!(chunks[0].schema(), chunks[1].schema());
            assert!(chunks[1].column_by_name("COUNT").unwrap().is_null(0));
            let combined =
                arrow_select::concat::concat_batches(&expected.schema(), &chunks).unwrap();
            assert_eq!(combined, expected);
            if hints.is_some() {
                assert_eq!(
                    combined
                        .column_by_name("COUNT")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(0),
                    "42"
                );
            } else {
                assert_eq!(
                    combined.schema().field(2).data_type(),
                    &arrow_schema::DataType::Int64
                );
            }
        }
    }

    #[test]
    fn streaming_history_preserves_diagnostics_including_metadata_only_failures() {
        use crate::engine::state::value_utils::{
            ResponseMetadata, METADATA_KEY_EID_DATA, METADATA_KEY_FIELD_EXCEPTIONS,
            METADATA_KEY_SECURITY_ERRORS,
        };

        let error = serde_json::json!({
            "category": "BAD_FLD", "code": 9,
            "subcategory": "INVALID_FIELD", "message": "Synthetic field error"
        });
        let first = response(
            EventType::PartialResponse,
            serde_json::json!({
                "security": "TEST Equity", "eidData": [11, 22],
                "fieldData": [{"date": "1970-01-02", "COUNT": 7}],
                "fieldExceptions": [{"fieldId": "TEXT", "errorInfo": error}]
            }),
        );
        let last = response(
            EventType::Response,
            serde_json::json!({
                "security": "DENIED Equity",
                "securityError": {
                    "category": "AUTHORIZATION", "code": 17,
                    "subcategory": "NOT_ENTITLED", "message": "Synthetic security error"
                },
                "fieldExceptions": [{"fieldId": "COUNT", "errorInfo": error}]
            }),
        );
        let fields = vec!["COUNT".into(), "TEXT".into()];
        let hints = Some(HashMap::from([
            ("COUNT".into(), "int64".into()),
            ("TEXT".into(), "string".into()),
        ]));
        let (reply, mut result) = oneshot::channel();
        let mut oneshot = HistDataState::with_format(
            fields.clone(),
            OutputFormat::Wide,
            LongMode::String,
            hints.clone(),
            reply,
        );
        let (sender, mut receiver) = mpsc::channel(2);
        let mut stream = HistDataStreamState::with_types(fields, hints, sender);
        let mut messages = first.event().messages();
        let message = messages.next().unwrap();
        oneshot.on_partial(&message);
        stream.on_partial(&message);
        let mut messages = last.event().messages();
        let message = messages.next().unwrap();
        oneshot.finish(&message);
        stream.finish(&message);
        let expected = result.try_recv().unwrap().unwrap();
        let chunks = [
            receiver.try_recv().unwrap().unwrap(),
            receiver.try_recv().unwrap().unwrap(),
        ];
        assert_eq!(chunks[1].num_rows(), 0);
        assert!(chunks[1]
            .schema_ref()
            .metadata()
            .contains_key(METADATA_KEY_SECURITY_ERRORS));
        let combined = arrow_select::concat::concat_batches(&expected.schema(), &chunks).unwrap();
        let combined = ResponseMetadata::union_of(&chunks).attach(combined);
        assert_eq!(combined, expected);
        let metadata = combined.schema_ref().metadata();
        let eids: serde_json::Value =
            serde_json::from_str(&metadata[METADATA_KEY_EID_DATA]).unwrap();
        assert_eq!(eids["TEST Equity"], serde_json::json!([11, 22]));
        let errors: serde_json::Value =
            serde_json::from_str(&metadata[METADATA_KEY_SECURITY_ERRORS]).unwrap();
        assert_eq!(errors["DENIED Equity"]["code"], 17);
        let exceptions: serde_json::Value =
            serde_json::from_str(&metadata[METADATA_KEY_FIELD_EXCEPTIONS]).unwrap();
        assert_eq!(exceptions["TEST Equity"][0]["field"], "TEXT");
        assert_eq!(exceptions["DENIED Equity"][0]["field"], "COUNT");
    }

    #[tokio::test]
    async fn full_history_channel_delivers_accepted_chunk_then_one_terminal_error() {
        let (sender, mut receiver) = mpsc::channel(1);
        let mut stream = HistDataStreamState::new(vec!["COUNT".into()], sender);
        let event = data_response(EventType::PartialResponse, 7);
        let mut messages = event.event().messages();
        let message = messages.next().unwrap();
        stream.on_partial(&message);
        stream.on_partial(&message);
        stream.on_partial(&message);
        stream.finish(&message);
        assert_eq!(receiver.recv().await.unwrap().unwrap().num_rows(), 1);
        let terminal = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(terminal, Err(BlpError::Internal { detail })
            if detail.contains("HistoricalDataRequest") && detail.contains("channel is full")));
        assert!(receiver.recv().await.is_none());
    }

    #[tokio::test]
    async fn full_history_channel_preserves_explicit_failure() {
        let (sender, mut receiver) = mpsc::channel(1);
        let mut stream = HistDataStreamState::new(vec!["COUNT".into()], sender);
        let event = data_response(EventType::PartialResponse, 7);
        let mut messages = event.event().messages();
        stream.on_partial(&messages.next().unwrap());
        stream.fail(BlpError::Internal {
            detail: "original failure".into(),
        });
        assert!(receiver.recv().await.unwrap().is_ok());
        let terminal = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(terminal, Err(BlpError::Internal { detail }) if detail == "original failure")
        );
        assert!(receiver.recv().await.is_none());
    }

    #[test]
    fn full_history_channel_outside_tokio_still_delivers_terminal_error() {
        let (sender, mut receiver) = mpsc::channel(1);
        let mut stream = HistDataStreamState::new(vec!["COUNT".into()], sender);
        let event = data_response(EventType::Response, 7);
        let mut messages = event.event().messages();
        let message = messages.next().unwrap();
        stream.on_partial(&message);
        stream.finish(&message);
        assert!(receiver.blocking_recv().unwrap().is_ok());
        assert!(matches!(
            receiver.blocking_recv(),
            Some(Err(BlpError::Internal { .. }))
        ));
        assert!(receiver.blocking_recv().is_none());
    }

    #[tokio::test]
    async fn closing_full_history_channel_releases_pending_terminal_send() {
        let (sender, mut receiver) = mpsc::channel(1);
        let mut stream = HistDataStreamState::new(vec!["COUNT".into()], sender);
        let event = data_response(EventType::Response, 7);
        let mut messages = event.event().messages();
        let message = messages.next().unwrap();
        stream.on_partial(&message);
        stream.finish(&message);
        receiver.close();
        assert!(receiver.recv().await.unwrap().is_ok());
        assert!(receiver.recv().await.is_none());
    }
}
