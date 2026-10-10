//! Unit tests for xbbg-async engine.
//!
//! These tests don't require a Bloomberg connection.

use crate::engine::state::{
    FieldKind, FieldLayout, FieldMeta, SubscriptionUpdate, subscription_update_to_record_batch,
};
use crate::engine::{EngineConfig, OutputFormat, OverflowPolicy, ServerAddr, Transport};
use arrow_schema::{DataType, TimeUnit};
use std::sync::Arc;

// =========================================================================
// Engine configuration tests
// =========================================================================

fn direct_servers(config: &EngineConfig) -> &[ServerAddr] {
    match &config.transport {
        Transport::Direct(servers) => servers.as_slice(),
        other => panic!("expected Direct transport, got {other}"),
    }
}

#[test]
fn test_engine_config_default_values() {
    let config = EngineConfig::default();

    let servers = direct_servers(&config);
    assert_eq!(servers.len(), 1);
    assert_eq!(servers[0].host, "localhost");
    assert_eq!(servers[0].port, 8194);
    assert!(servers[0].proxy.is_none());
    assert!(config.max_event_queue_size > 0);
    assert!(config.command_queue_size > 0);
    assert!(config.subscription_flush_threshold > 0);
    assert!(config.subscription_stream_capacity > 0);
}

#[test]
fn test_engine_config_custom_values() {
    let config = EngineConfig {
        transport: Transport::Direct(vec![ServerAddr::new("bloomberg.example.com", 8195)]),
        max_event_queue_size: 20000,
        command_queue_size: 512,
        subscription_flush_threshold: 200,
        subscription_stream_capacity: 2048,
        overflow_policy: OverflowPolicy::Block,
        ..Default::default()
    };

    let servers = direct_servers(&config);
    assert_eq!(servers[0].host, "bloomberg.example.com");
    assert_eq!(servers[0].port, 8195);
    assert_eq!(config.max_event_queue_size, 20000);
    assert_eq!(config.command_queue_size, 512);
    assert_eq!(config.overflow_policy, OverflowPolicy::Block);
}

// =========================================================================
// Overflow policy tests
// =========================================================================

#[test]
fn test_overflow_policy_default() {
    let policy = OverflowPolicy::default();
    assert_eq!(policy, OverflowPolicy::DropNewest);
}

#[test]
fn test_overflow_policy_variants() {
    assert_eq!(OverflowPolicy::DropNewest, OverflowPolicy::DropNewest);
    assert_eq!(OverflowPolicy::Block, OverflowPolicy::Block);

    assert_ne!(OverflowPolicy::DropNewest, OverflowPolicy::Block);
}

#[test]
fn test_overflow_policy_clone() {
    let policy = OverflowPolicy::Block;
    let cloned = policy;
    assert_eq!(policy, cloned);
}

#[test]
fn test_overflow_policy_debug() {
    let policy = OverflowPolicy::DropNewest;
    let debug_str = format!("{:?}", policy);
    assert!(debug_str.contains("DropNewest"));
}

// =========================================================================
// Output format tests
// =========================================================================

#[test]
fn test_output_format_default() {
    let format = OutputFormat::default();
    assert_eq!(format, OutputFormat::Long);
}

#[test]
fn test_output_format_variants() {
    assert_eq!(OutputFormat::Wide, OutputFormat::Wide);
    assert_eq!(OutputFormat::Long, OutputFormat::Long);
    assert_ne!(OutputFormat::Wide, OutputFormat::Long);
}

#[test]
fn test_output_format_clone() {
    let format = OutputFormat::Wide;
    let cloned = format;
    assert_eq!(format, cloned);
}

// =========================================================================
// BlpAsyncError tests
// =========================================================================

use crate::errors::BlpAsyncError;
use xbbg_core::BlpError;

#[test]
fn test_blp_async_error_from_blp_error_session_start() {
    let blp_err = BlpError::SessionStart {
        source: None,
        label: Some("test label".to_string()),
    };

    let async_err: BlpAsyncError = blp_err.into();

    // Should be wrapped in Blp variant
    assert!(matches!(async_err, BlpAsyncError::Blp(_)));

    // Error message should contain the original error info
    let msg = async_err.to_string();
    assert!(
        msg.contains("session start"),
        "Expected 'session start' in: {}",
        msg
    );
}

#[test]
fn test_blp_async_error_from_blp_error_open_service() {
    let blp_err = BlpError::OpenService {
        service: "//blp/refdata".to_string(),
        source: None,
        label: Some("connection refused".to_string()),
    };

    let async_err: BlpAsyncError = blp_err.into();

    assert!(matches!(async_err, BlpAsyncError::Blp(_)));
    let msg = async_err.to_string();
    assert!(
        msg.contains("open service"),
        "Expected 'open service' in: {}",
        msg
    );
}

#[test]
fn test_blp_async_error_from_blp_error_request_failure() {
    let blp_err = BlpError::RequestFailure {
        service: "//blp/refdata".to_string(),
        operation: Some("ReferenceDataRequest".to_string()),
        cid: None,
        label: Some("invalid security".to_string()),
        request_id: Some("req-123".to_string()),
        source: None,
    };

    let async_err: BlpAsyncError = blp_err.into();

    assert!(matches!(async_err, BlpAsyncError::Blp(_)));
    let msg = async_err.to_string();
    assert!(
        msg.contains("request failed"),
        "Expected 'request failed' in: {}",
        msg
    );
}

#[test]
fn test_blp_async_error_from_blp_error_invalid_argument() {
    let blp_err = BlpError::InvalidArgument {
        detail: "securities cannot be empty".to_string(),
    };

    let async_err: BlpAsyncError = blp_err.into();

    assert!(matches!(async_err, BlpAsyncError::Blp(_)));
    let msg = async_err.to_string();
    assert!(
        msg.contains("invalid argument"),
        "Expected 'invalid argument' in: {}",
        msg
    );
}

#[test]
fn test_blp_async_error_from_blp_error_timeout() {
    let blp_err = BlpError::Timeout;

    let async_err: BlpAsyncError = blp_err.into();

    assert!(matches!(async_err, BlpAsyncError::Blp(_)));
    let msg = async_err.to_string();
    assert!(
        msg.contains("timed out"),
        "Expected 'timed out' in: {}",
        msg
    );
}

#[test]
fn test_blp_async_error_from_blp_error_internal() {
    let blp_err = BlpError::Internal {
        detail: "unexpected state".to_string(),
    };

    let async_err: BlpAsyncError = blp_err.into();

    assert!(matches!(async_err, BlpAsyncError::Blp(_)));
    let msg = async_err.to_string();
    assert!(
        msg.contains("internal error"),
        "Expected 'internal error' in: {}",
        msg
    );
}

#[test]
fn test_blp_async_error_surviving_display_contract() {
    use std::error::Error;

    for (error, expected) in [
        (
            BlpAsyncError::Internal("engine shutdown".into()),
            "internal error: engine shutdown",
        ),
        (
            BlpAsyncError::ConfigError {
                detail: "synthetic config failure".into(),
            },
            "configuration error: synthetic config failure",
        ),
        (BlpAsyncError::ChannelClosed, "channel closed"),
        (
            BlpAsyncError::AllWorkersDown { pool_size: 2 },
            "all 2 request workers are dead — no healthy worker available",
        ),
        (BlpError::Timeout.into(), "operation timed out"),
        (
            BlpError::Internal {
                detail: "session connection dropped (worker=2)".into(),
            }
            .into(),
            "internal error: session connection dropped (worker=2)",
        ),
        (
            BlpError::SubscriptionDataLoss {
                topic: "SYNTHETIC Equity".into(),
                detail: "synthetic overflow".into(),
            }
            .into(),
            "subscription data loss for SYNTHETIC Equity: synthetic overflow",
        ),
    ] {
        assert_eq!(error.to_string(), expected);
        assert!(error.source().is_none());
    }
}

#[test]
fn test_blp_async_error_transparently_preserves_display_and_source_chain() {
    use std::error::Error;

    #[derive(Debug, thiserror::Error)]
    #[error("synthetic transport failure")]
    struct TransportFailure {
        #[source]
        source: std::io::Error,
    }

    fn source() -> Option<Box<dyn Error + Send + Sync>> {
        Some(Box::new(TransportFailure {
            source: std::io::Error::other("synthetic root cause"),
        }))
    }

    for error in [
        BlpError::SessionStart {
            source: source(),
            label: Some("synthetic session failure".into()),
        },
        BlpError::OpenService {
            service: "//blp/refdata".into(),
            source: source(),
            label: Some("synthetic service failure".into()),
        },
        BlpError::RequestFailure {
            service: "//blp/refdata".into(),
            operation: Some("ReferenceDataRequest".into()),
            cid: None,
            label: Some("synthetic request failure".into()),
            request_id: None,
            source: source(),
        },
    ] {
        let expected_display = error.to_string();
        let async_error = BlpAsyncError::from(error);
        assert_eq!(async_error.to_string(), expected_display);
        let source = async_error.source().expect("core error source");
        assert!(source.is::<TransportFailure>());
        assert_eq!(source.to_string(), "synthetic transport failure");
        let root = source.source().expect("nested source");
        assert!(root.is::<std::io::Error>());
        assert_eq!(root.to_string(), "synthetic root cause");
        assert!(root.source().is_none());
    }
}

#[test]
fn test_blp_async_error_blp_preserves_context() {
    // Test that wrapping BlpError preserves all structured context
    let blp_err = BlpError::RequestFailure {
        service: "//blp/refdata".to_string(),
        operation: Some("HistoricalDataRequest".to_string()),
        cid: None,
        label: Some("field not found: INVALID_FIELD".to_string()),
        request_id: Some("correlation-456".to_string()),
        source: None,
    };

    let async_err = BlpAsyncError::Blp(blp_err);

    // Extract the inner BlpError to verify context is preserved
    if let BlpAsyncError::Blp(inner) = async_err {
        if let BlpError::RequestFailure {
            service,
            operation,
            label,
            request_id,
            ..
        } = inner
        {
            assert_eq!(service, "//blp/refdata");
            assert_eq!(operation, Some("HistoricalDataRequest".to_string()));
            assert_eq!(label, Some("field not found: INVALID_FIELD".to_string()));
            assert_eq!(request_id, Some("correlation-456".to_string()));
        } else {
            panic!("Expected RequestFailure variant");
        }
    } else {
        panic!("Expected Blp variant");
    }
}

#[test]
fn test_blp_async_error_is_send_sync() {
    // BlpAsyncError must be Send + Sync for use across async boundaries
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<BlpAsyncError>();
}

#[test]
fn test_subscription_arrow_adapter_emits_utc_timestamp_column() {
    let update = SubscriptionUpdate {
        timestamp_us: 1_717_242_600_000_000,
        topic_id: 0,
        topic: Arc::from("AAPL US Equity"),
        layout: Arc::new(FieldLayout::new(
            1,
            vec![FieldMeta::new("LAST_PRICE", 0, FieldKind::Unknown)],
        )),
        values: smallvec::SmallVec::new(),
    };

    let batch = subscription_update_to_record_batch(&update)
        .expect("subscription update should adapt to a RecordBatch");
    let expected = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));

    assert_eq!(batch.schema().field(0).data_type(), &expected);
    assert_eq!(batch.column(0).data_type(), &expected);
}
