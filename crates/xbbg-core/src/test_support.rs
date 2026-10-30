//! SDK-backed message fixtures for downstream tests, without a Bloomberg session.

use std::ffi::CString;
use std::ptr::{self, NonNull};

use blpapi_sys as sdk;

use crate::{BlpError, CorrelationId, Event, EventType, Name, Result};

/// Keeps a test event's deserialized schema alive until after the event is released.
pub struct TestEvent {
    event: Event,
    _service: Option<TestService>,
}

impl TestEvent {
    /// Construct one subscription message using an explicit SDK service schema.
    ///
    /// TestUtil materializes every field declared in the schema, including omitted
    /// nullable JSON fields. To represent absence, omit the field from the schema.
    pub fn subscription(
        schema: &str,
        message_type: &str,
        format: impl FnOnce(&mut TestMessageFormatter),
    ) -> Self {
        Self::message(
            Some(schema),
            EventType::SubscriptionData,
            message_type,
            &[],
            format,
        )
    }

    /// Construct a message with an explicit schema, event kind and correlation IDs.
    pub fn with_schema(
        schema: &str,
        event_type: EventType,
        message_type: &str,
        correlation_ids: &[i64],
        format: impl FnOnce(&mut TestMessageFormatter),
    ) -> Self {
        Self::message(
            Some(schema),
            event_type,
            message_type,
            correlation_ids,
            format,
        )
    }

    /// Construct a message while returning schema, SDK and formatting failures.
    ///
    /// Use this rather than the assertion-based helpers for externally supplied
    /// fixture input. The callback must use checked formatter methods such as
    /// [`TestMessageFormatter::try_json`] to propagate formatting errors.
    pub fn try_with_schema(
        schema: &str,
        event_type: EventType,
        message_type: &str,
        correlation_ids: &[i64],
        format: impl FnOnce(&mut TestMessageFormatter) -> Result<()>,
    ) -> Result<Self> {
        Self::try_message(
            Some(schema),
            event_type,
            message_type,
            correlation_ids,
            format,
        )
    }

    /// Construct an SDK-defined administrative or status message.
    pub fn admin(
        event_type: EventType,
        message_type: &str,
        correlation_ids: &[i64],
        format: impl FnOnce(&mut TestMessageFormatter),
    ) -> Self {
        Self::message(None, event_type, message_type, correlation_ids, format)
    }

    fn message(
        schema: Option<&str>,
        event_type: EventType,
        message_type: &str,
        correlation_ids: &[i64],
        format: impl FnOnce(&mut TestMessageFormatter),
    ) -> Self {
        Self::try_message(
            schema,
            event_type,
            message_type,
            correlation_ids,
            |formatter| {
                format(formatter);
                Ok(())
            },
        )
        .expect("construct test event")
    }

    fn try_message(
        schema: Option<&str>,
        event_type: EventType,
        message_type: &str,
        correlation_ids: &[i64],
        format: impl FnOnce(&mut TestMessageFormatter) -> Result<()>,
    ) -> Result<Self> {
        let name = Name::new(message_type).ok_or_else(|| BlpError::InvalidArgument {
            detail: "test message type could not be interned".into(),
        })?;
        let mut definition = ptr::null_mut();
        let service = if let Some(schema) = schema {
            if schema.bytes().any(|byte| byte == 0 || !byte.is_ascii()) {
                return Err(BlpError::InvalidArgument {
                    detail: "test service schema must be ASCII without embedded NULs".into(),
                });
            }
            let mut service = ptr::null_mut();
            // SAFETY: schema is readable for its stated length; the output is initialized on success.
            check_result(
                unsafe {
                    sdk::blpapi_TestUtil_deserializeService(
                        schema.as_ptr().cast(),
                        schema.len(),
                        &mut service,
                    )
                },
                "deserialize test service",
            )?;
            let service = TestService(checked_pointer(service, "deserialize test service")?);
            // SAFETY: service and name remain alive while the borrowed definition is used.
            check_result(
                unsafe {
                    sdk::blpapi_Service_getEventDefinition(
                        service.0.as_ptr(),
                        &mut definition,
                        ptr::null(),
                        name.as_ptr(),
                    )
                },
                "test message definition",
            )?;
            Some(service)
        } else {
            // SAFETY: the SDK owns its built-in definitions; name remains live for the call.
            check_result(
                unsafe {
                    sdk::blpapi_TestUtil_getAdminMessageDefinition(&mut definition, name.as_ptr())
                },
                "test administrative definition",
            )?;
            None
        };
        let definition = checked_pointer(definition, "test message definition")?;
        let mut raw_event = ptr::null_mut();
        // SAFETY: the successful call transfers one owned event reference.
        check_result(
            unsafe { sdk::blpapi_TestUtil_createEvent(&mut raw_event, event_type.to_raw()) },
            "create test event",
        )?;
        let raw_event = checked_pointer(raw_event, "create test event")?;
        // SAFETY: adopt that owned reference exactly once using the core ownership boundary.
        let event = unsafe { Event::from_raw(raw_event.as_ptr()) };

        Self::try_append_raw(raw_event, definition.as_ptr(), correlation_ids, format)?;
        Ok(Self {
            event,
            _service: service,
        })
    }

    /// Append another message to the same SDK event. A mutable borrow prevents
    /// existing message views from being used while the event is extended.
    pub fn append_message(
        &mut self,
        message_type: &str,
        correlation_ids: &[i64],
        format: impl FnOnce(&mut TestMessageFormatter),
    ) {
        let name = Name::get_or_intern(message_type);
        let mut definition = ptr::null_mut();
        // SAFETY: this fixture keeps its service alive; built-in definitions are SDK-owned.
        let result = unsafe {
            if let Some(service) = &self._service {
                sdk::blpapi_Service_getEventDefinition(
                    service.0.as_ptr(),
                    &mut definition,
                    ptr::null(),
                    name.as_ptr(),
                )
            } else {
                sdk::blpapi_TestUtil_getAdminMessageDefinition(&mut definition, name.as_ptr())
            }
        };
        assert_eq!(result, 0, "test message definition");
        Self::append_raw(
            NonNull::new(self.event.as_ptr()).expect("live test event"),
            definition,
            correlation_ids,
            format,
        );
    }

    fn append_raw(
        event: NonNull<sdk::blpapi_Event_t>,
        definition: *mut sdk::blpapi_SchemaElementDefinition_t,
        correlation_ids: &[i64],
        format: impl FnOnce(&mut TestMessageFormatter),
    ) {
        Self::try_append_raw(event, definition, correlation_ids, |formatter| {
            format(formatter);
            Ok(())
        })
        .expect("append test message");
    }

    fn try_append_raw(
        event: NonNull<sdk::blpapi_Event_t>,
        definition: *mut sdk::blpapi_SchemaElementDefinition_t,
        correlation_ids: &[i64],
        format: impl FnOnce(&mut TestMessageFormatter) -> Result<()>,
    ) -> Result<()> {
        let mut properties = ptr::null_mut();
        // SAFETY: properties is an initialized output pointer.
        check_result(
            unsafe { sdk::blpapi_MessageProperties_create(&mut properties) },
            "create test message properties",
        )?;
        let properties =
            TestMessageProperties(checked_pointer(properties, "create test properties")?);
        if !correlation_ids.is_empty() {
            let correlation_ids: Vec<_> = correlation_ids
                .iter()
                .map(|&id| CorrelationId::new_int(id).to_ffi())
                .collect();
            // SAFETY: the SDK copies these initialized IDs into the live properties.
            check_result(
                unsafe {
                    sdk::blpapi_MessageProperties_setCorrelationIds(
                        properties.0.as_ptr(),
                        correlation_ids.as_ptr(),
                        correlation_ids.len(),
                    )
                },
                "set test correlation IDs",
            )?;
        }
        let mut formatter = ptr::null_mut();
        // SAFETY: the event owner, schema owner and properties outlive this formatter.
        check_result(
            unsafe {
                sdk::blpapi_TestUtil_appendMessage(
                    &mut formatter,
                    event.as_ptr(),
                    definition,
                    properties.0.as_ptr(),
                )
            },
            "append test message",
        )?;
        let mut formatter = TestMessageFormatter {
            raw: checked_pointer(formatter, "append test message")?,
            mode: FormattingMode::Empty,
        };
        format(&mut formatter)
    }

    pub fn event(&self) -> &Event {
        &self.event
    }
}

struct TestService(NonNull<sdk::blpapi_Service_t>);

impl Drop for TestService {
    fn drop(&mut self) {
        // SAFETY: TestUtil gave this owner one service reference; the event drops first.
        unsafe { sdk::blpapi_Service_release(self.0.as_ptr()) };
    }
}

struct TestMessageProperties(NonNull<sdk::blpapi_MessageProperties_t>);

impl Drop for TestMessageProperties {
    fn drop(&mut self) {
        // SAFETY: this owns the successfully created properties pointer.
        unsafe { sdk::blpapi_MessageProperties_destroy(self.0.as_ptr()) };
    }
}

enum FormattingMode {
    Empty,
    Typed,
    JsonAttempted,
}

pub struct TestMessageFormatter {
    raw: NonNull<sdk::blpapi_MessageFormatter_t>,
    mode: FormattingMode,
}

impl TestMessageFormatter {
    pub fn json(&mut self, value: &str) {
        self.try_json(value).expect("format test JSON");
    }

    /// Format JSON without panicking on invalid input or an SDK rejection.
    ///
    /// JSON must be the first and only formatting operation on this message.
    /// Even a rejected SDK JSON attempt prevents subsequent formatting, as
    /// required by the SDK; invalid sequences return an error before FFI.
    pub fn try_json(&mut self, value: &str) -> Result<()> {
        if !matches!(self.mode, FormattingMode::Empty) {
            return Err(BlpError::InvalidArgument {
                detail: "JSON must be the first and only test-message formatting operation".into(),
            });
        }
        let value = CString::new(value).map_err(|_| BlpError::InvalidArgument {
            detail: "test JSON contains an interior NUL".into(),
        })?;
        self.mode = FormattingMode::JsonAttempted;
        // SAFETY: formatter is live and value is a valid C string for the call.
        check_result(
            unsafe {
                sdk::blpapi_MessageFormatter_FormatMessageJson(self.raw.as_ptr(), value.as_ptr())
            },
            "format test JSON",
        )
    }

    pub fn char(&mut self, field: &str, value: Option<u8>) {
        self.try_char(field, value).expect("format test Char");
    }

    /// Set a Char or null field without permitting typed formatting after JSON.
    pub fn try_char(&mut self, field: &str, value: Option<u8>) -> Result<()> {
        self.begin_typed()?;
        let field = Name::new(field).ok_or_else(|| BlpError::InvalidArgument {
            detail: "test field name could not be interned".into(),
        })?;
        // SAFETY: typed mode is allowed and the formatter/name remain live.
        let result = unsafe {
            match value {
                Some(value) => sdk::blpapi_MessageFormatter_setValueChar(
                    self.raw.as_ptr(),
                    field.as_ptr(),
                    value as std::ffi::c_char,
                ),
                None => {
                    sdk::blpapi_MessageFormatter_setValueNull(self.raw.as_ptr(), field.as_ptr())
                }
            }
        };
        check_result(result, "format test Char")
    }

    pub fn datetime(&mut self, field: &str, value: &sdk::blpapi_HighPrecisionDatetime_t) {
        self.try_datetime(field, value)
            .expect("format test datetime");
    }

    /// Set a datetime without permitting typed formatting after JSON.
    pub fn try_datetime(
        &mut self,
        field: &str,
        value: &sdk::blpapi_HighPrecisionDatetime_t,
    ) -> Result<()> {
        self.begin_typed()?;
        let field = Name::new(field).ok_or_else(|| BlpError::InvalidArgument {
            detail: "test field name could not be interned".into(),
        })?;
        // SAFETY: typed mode is allowed and formatter/name/datetime remain live.
        check_result(
            unsafe {
                sdk::blpapi_MessageFormatter_setValueHighPrecisionDatetime(
                    self.raw.as_ptr(),
                    field.as_ptr(),
                    value,
                )
            },
            "format test datetime",
        )
    }

    fn begin_typed(&mut self) -> Result<()> {
        if matches!(self.mode, FormattingMode::JsonAttempted) {
            return Err(BlpError::InvalidArgument {
                detail: "test-message formatting is not allowed after a JSON attempt".into(),
            });
        }
        self.mode = FormattingMode::Typed;
        Ok(())
    }
}

impl Drop for TestMessageFormatter {
    fn drop(&mut self) {
        // SAFETY: this owns the formatter created by appendMessage.
        unsafe { sdk::blpapi_MessageFormatter_destroy(self.raw.as_ptr()) };
    }
}

fn check_result(rc: i32, operation: &str) -> Result<()> {
    if rc != 0 {
        return Err(BlpError::Internal {
            detail: format!("{operation} failed with rc={rc}"),
        });
    }
    Ok(())
}

fn checked_pointer<T>(pointer: *mut T, operation: &str) -> Result<NonNull<T>> {
    NonNull::new(pointer).ok_or_else(|| BlpError::Internal {
        detail: format!("{operation} returned a null pointer"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: &str = r#"<ServiceDefinition name="xbbg.test" version="1.0.0.0">
        <service name="//xbbg/test" version="1.0.0.0">
            <event name="MarketDataEvents" eventType="Update"/>
        </service><schema><sequenceType name="Update">
            <element name="NUMBER" type="Float64" minOccurs="0"/>
        </sequenceType></schema></ServiceDefinition>"#;

    #[test]
    fn checked_fixture_returns_schema_and_message_type_errors() {
        let schema_error = TestEvent::try_with_schema(
            "<InvalidServiceDefinition/>",
            EventType::SubscriptionData,
            "MarketDataEvents",
            &[],
            |_| Ok(()),
        );
        assert!(matches!(
            schema_error,
            Err(BlpError::Internal { detail }) if detail.contains("deserialize test service")
        ));
        let name_error = TestEvent::try_with_schema(
            SCHEMA,
            EventType::SubscriptionData,
            "Market\0Data",
            &[],
            |_| Ok(()),
        );
        assert!(matches!(name_error, Err(BlpError::InvalidArgument { .. })));
        for schema in ["\0", "non-ASCII \u{e9}"] {
            let invalid_schema = TestEvent::try_with_schema(
                schema,
                EventType::SubscriptionData,
                "MarketDataEvents",
                &[],
                |_| Ok(()),
            );
            assert!(matches!(
                invalid_schema,
                Err(BlpError::InvalidArgument { .. })
            ));
        }
    }

    #[test]
    fn checked_fixture_returns_formatter_errors_and_accepts_valid_json_afterward() {
        let malformed = TestEvent::try_with_schema(
            SCHEMA,
            EventType::SubscriptionData,
            "MarketDataEvents",
            &[],
            |formatter| formatter.try_json("not-json"),
        );
        assert!(matches!(
            malformed,
            Err(BlpError::Internal { detail }) if detail.contains("format test JSON")
        ));
        let interior_nul = TestEvent::try_with_schema(
            SCHEMA,
            EventType::SubscriptionData,
            "MarketDataEvents",
            &[],
            |formatter| formatter.try_json("{\0}"),
        );
        assert!(matches!(
            interior_nul,
            Err(BlpError::InvalidArgument { .. })
        ));
        assert!(
            TestEvent::try_with_schema(
                SCHEMA,
                EventType::SubscriptionData,
                "MarketDataEvents",
                &[],
                |formatter| formatter.try_json(r#"{"NUMBER":2.5}"#),
            )
            .is_ok()
        );
    }

    #[test]
    fn checked_formatter_rejects_repeated_and_mixed_json_before_ffi() {
        let formatted = TestEvent::try_with_schema(
            SCHEMA,
            EventType::SubscriptionData,
            "MarketDataEvents",
            &[],
            |formatter| {
                formatter.try_json(r#"{"NUMBER":2.5}"#)?;
                assert!(matches!(
                    formatter.try_json(r#"{"NUMBER":3.5}"#),
                    Err(BlpError::InvalidArgument { .. })
                ));
                assert!(matches!(
                    formatter.try_char("NUMBER", None),
                    Err(BlpError::InvalidArgument { .. })
                ));
                Ok(())
            },
        );
        assert!(formatted.is_ok());
        let rejected_json = TestEvent::try_with_schema(
            SCHEMA,
            EventType::SubscriptionData,
            "MarketDataEvents",
            &[],
            |formatter| {
                let error = formatter.try_json("not-json").unwrap_err();
                assert!(matches!(
                    formatter.try_json(r#"{"NUMBER":3.5}"#),
                    Err(BlpError::InvalidArgument { .. })
                ));
                assert!(matches!(
                    formatter.try_char("NUMBER", None),
                    Err(BlpError::InvalidArgument { .. })
                ));
                Err(error)
            },
        );
        assert!(matches!(rejected_json, Err(BlpError::Internal { .. })));
        let typed = TestEvent::try_with_schema(
            SCHEMA,
            EventType::SubscriptionData,
            "MarketDataEvents",
            &[],
            |formatter| {
                formatter.try_char("NUMBER", None)?;
                assert!(matches!(
                    formatter.try_json(r#"{"NUMBER":3.5}"#),
                    Err(BlpError::InvalidArgument { .. })
                ));
                Ok(())
            },
        );
        assert!(typed.is_ok());
    }
}
