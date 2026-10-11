//! Streaming intraday bar (bdib) state with Arrow builders.
//!
//! Unlike IntradayBarState, this state yields chunks immediately via a channel
//! instead of accumulating all data until the final response.
//!
//! Extracts directly from Bloomberg Elements without JSON intermediate.

use arrow_array::RecordBatch;
use tokio::sync::mpsc;

use super::histdata_stream::ResponseStream;
use super::intradaybar::IntradayBarResponse;
use super::value_utils::top_level_response_error;
use xbbg_core::{BlpError, Message};

/// Streaming state for an intraday bar request (bdib).
///
/// Chunks share the accumulated response's decoder and non-nullable ticker
/// schema. Channel overflow yields a terminal error after accepted chunks,
/// rather than silently dropping data or blocking the SDK dispatcher.
pub struct IntradayBarStreamState {
    response: IntradayBarResponse,
    stream: ResponseStream,
}

impl IntradayBarStreamState {
    /// Create a new streaming intraday bar state.
    pub fn new(ticker: String, stream: mpsc::Sender<Result<RecordBatch, BlpError>>) -> Self {
        Self {
            response: IntradayBarResponse::new(ticker),
            stream: ResponseStream::new(stream, "IntradayBarRequest"),
        }
    }

    /// Process a PARTIAL_RESPONSE message and yield a chunk.
    pub fn on_partial(&mut self, msg: &Message) {
        if self.stream.is_closed() {
            return;
        }
        if let Some(error) = top_level_response_error(msg, "//blp/refdata", "IntradayBarRequest") {
            self.stream.send(Err(error));
            return;
        }
        if self.response.process_message(msg) {
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
    use crate::engine::state::IntradayBarState;
    use arrow_array::{Array, Float64Array, Int32Array, TimestampMicrosecondArray};
    use tokio::sync::oneshot;
    use xbbg_core::EventType;
    use xbbg_core::test_support::TestEvent;

    fn response(event_type: EventType, data: serde_json::Value) -> TestEvent {
        let schema = r#"<ServiceDefinition name="xbbg.test.bars" version="1.0.0.0">
            <service name="//xbbg/test/bars" version="1.0.0.0">
                <event name="IntradayBarResponse" eventType="BarResponse"/>
            </service>
            <schema>
                <sequenceType name="BarResponse">
                    <element name="barData" type="BarData"/>
                </sequenceType>
                <sequenceType name="BarData">
                    <element name="eidData" type="Int32" minOccurs="0" maxOccurs="unbounded"/>
                    <element name="barTickData" type="BarRow" minOccurs="0" maxOccurs="unbounded"/>
                </sequenceType>
                <sequenceType name="BarRow">
                    <element name="time" type="Datetime" minOccurs="0"/>
                    <element name="open" type="Float64" minOccurs="0"/>
                    <element name="high" type="Float64" minOccurs="0"/>
                    <element name="low" type="Float64" minOccurs="0"/>
                    <element name="close" type="Float64" minOccurs="0"/>
                    <element name="volume" type="Int64" minOccurs="0"/>
                    <element name="numEvents" type="Int64" minOccurs="0"/>
                    <element name="value" type="Float64" minOccurs="0"/>
                </sequenceType>
            </schema>
        </ServiceDefinition>"#;
        TestEvent::with_schema(
            schema,
            event_type,
            "IntradayBarResponse",
            &[],
            |formatter| formatter.json(&serde_json::json!({"barData": data}).to_string()),
        )
    }

    #[test]
    fn streaming_bars_match_oneshot_schema_values_and_nulls() {
        let (reply, mut result) = oneshot::channel();
        let mut oneshot = IntradayBarState::new("TEST Equity".into(), "TRADE".into(), 5, reply);
        assert_eq!(oneshot.event_type(), "TRADE");
        assert_eq!(oneshot.interval(), 5);
        let (sender, mut receiver) = mpsc::channel(2);
        let mut stream = IntradayBarStreamState::new("TEST Equity".into(), sender);
        let first = response(
            EventType::PartialResponse,
            serde_json::json!({
                "barTickData": [{
                    "time": "1970-01-01T00:00:01Z",
                    "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
                    "volume": 7, "numEvents": 3, "value": 10.5
                }]
            }),
        );
        let last = response(
            EventType::Response,
            serde_json::json!({
                "barTickData": [{"open": 2.0, "volume": null, "numEvents": null}]
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
        for chunk in &chunks {
            assert_eq!(chunk.schema(), expected.schema());
            assert!(!chunk.schema().field(0).is_nullable());
        }
        let combined = arrow_select::concat::concat_batches(&expected.schema(), &chunks).unwrap();
        assert_eq!(combined, expected);
        assert_eq!(
            combined
                .column_by_name("time")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            1_000_000,
        );
        assert_eq!(
            combined
                .column_by_name("volume")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            7.0,
        );
        assert_eq!(
            combined
                .column_by_name("numEvents")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            3,
        );
        for field in [
            "time",
            "high",
            "low",
            "close",
            "volume",
            "numEvents",
            "value",
        ] {
            assert!(
                combined.column_by_name(field).unwrap().is_null(1),
                "{field}"
            );
        }
    }

    #[test]
    fn streaming_bars_preserve_metadata_without_rows() {
        let event = response(
            EventType::Response,
            serde_json::json!({"eidData": [11, 22]}),
        );
        let mut messages = event.event().messages();
        let message = messages.next().unwrap();
        let (reply, mut result) = oneshot::channel();
        IntradayBarState::new("TEST Equity".into(), "TRADE".into(), 1, reply).finish(&message);
        let (sender, mut receiver) = mpsc::channel(1);
        IntradayBarStreamState::new("TEST Equity".into(), sender).finish(&message);
        let expected = result.try_recv().unwrap().unwrap();
        let actual = receiver.try_recv().unwrap().unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.num_rows(), 0);
        assert_eq!(
            actual.schema_ref().metadata()["xbbg.eid_data"],
            r#"{"TEST Equity":[11,22]}"#,
        );
    }

    #[tokio::test]
    async fn full_bar_channel_preserves_terminal_error_after_accepted_chunk() {
        let event = response(
            EventType::PartialResponse,
            serde_json::json!({
                "barTickData": [{"open": 1.0}]
            }),
        );
        let mut messages = event.event().messages();
        let message = messages.next().unwrap();
        let (sender, mut receiver) = mpsc::channel(1);
        let mut stream = IntradayBarStreamState::new("TEST Equity".into(), sender);
        stream.on_partial(&message);
        stream.on_partial(&message);
        stream.finish(&message);
        assert!(receiver.recv().await.unwrap().is_ok());
        let terminal = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(terminal, Err(BlpError::Internal { detail })
            if detail.contains("IntradayBarRequest") && detail.contains("channel is full")));
        assert!(receiver.recv().await.is_none());
    }
}
