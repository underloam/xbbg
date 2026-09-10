//! SDK-backed message fixtures for downstream tests, without a Bloomberg session.

use std::ffi::CString;
use std::ptr::{self, NonNull};

use blpapi_sys as sdk;

use crate::{CorrelationId, Event, EventType, Name};

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
        let name = Name::get_or_intern(message_type);
        let mut definition = ptr::null_mut();
        let service = if let Some(schema) = schema {
            let mut service = ptr::null_mut();
            // SAFETY: schema is readable for its stated length; the output is initialized on success.
            assert_eq!(
                unsafe {
                    sdk::blpapi_TestUtil_deserializeService(
                        schema.as_ptr().cast(),
                        schema.len(),
                        &mut service,
                    )
                },
                0,
                "deserialize test service"
            );
            let service = TestService(NonNull::new(service).expect("test service"));
            // SAFETY: service and name remain alive while the borrowed definition is used.
            assert_eq!(
                unsafe {
                    sdk::blpapi_Service_getEventDefinition(
                        service.0.as_ptr(),
                        &mut definition,
                        ptr::null(),
                        name.as_ptr(),
                    )
                },
                0,
                "test message definition"
            );
            Some(service)
        } else {
            // SAFETY: the SDK owns its built-in definitions; name remains live for the call.
            assert_eq!(
                unsafe {
                    sdk::blpapi_TestUtil_getAdminMessageDefinition(&mut definition, name.as_ptr())
                },
                0,
                "test administrative definition"
            );
            None
        };

        let mut raw_event = ptr::null_mut();
        // SAFETY: the successful call transfers one owned event reference.
        assert_eq!(
            unsafe { sdk::blpapi_TestUtil_createEvent(&mut raw_event, event_type.to_raw()) },
            0,
            "create test event"
        );
        let raw_event = NonNull::new(raw_event).expect("test event");
        // SAFETY: adopt that owned reference exactly once using the core ownership boundary.
        let event = unsafe { Event::from_raw(raw_event.as_ptr()) };

        let mut properties = ptr::null_mut();
        // SAFETY: properties is an initialized output pointer.
        assert_eq!(
            unsafe { sdk::blpapi_MessageProperties_create(&mut properties) },
            0
        );
        let properties = TestMessageProperties(NonNull::new(properties).expect("test properties"));
        if !correlation_ids.is_empty() {
            let correlation_ids: Vec<_> = correlation_ids
                .iter()
                .map(|&id| CorrelationId::new_int(id).to_ffi())
                .collect();
            // SAFETY: the SDK copies these initialized IDs into the live properties.
            assert_eq!(
                unsafe {
                    sdk::blpapi_MessageProperties_setCorrelationIds(
                        properties.0.as_ptr(),
                        correlation_ids.as_ptr(),
                        correlation_ids.len(),
                    )
                },
                0,
                "set test correlation IDs"
            );
        }
        let mut formatter = ptr::null_mut();
        // SAFETY: event, schema definition and properties outlive the formatter.
        assert_eq!(
            unsafe {
                sdk::blpapi_TestUtil_appendMessage(
                    &mut formatter,
                    raw_event.as_ptr(),
                    definition,
                    properties.0.as_ptr(),
                )
            },
            0,
            "append test message"
        );
        let mut formatter = TestMessageFormatter(NonNull::new(formatter).expect("test formatter"));
        format(&mut formatter);
        Self {
            event,
            _service: service,
        }
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

pub struct TestMessageFormatter(NonNull<sdk::blpapi_MessageFormatter_t>);

impl TestMessageFormatter {
    pub fn json(&mut self, value: &str) {
        let value = CString::new(value).expect("fixture JSON has no NUL");
        // SAFETY: formatter is live and value is a valid C string for the call.
        assert_eq!(
            unsafe {
                sdk::blpapi_MessageFormatter_FormatMessageJson(self.0.as_ptr(), value.as_ptr())
            },
            0,
            "format test JSON"
        );
    }

    pub fn char(&mut self, field: &str, value: Option<u8>) {
        let field = Name::get_or_intern(field);
        // SAFETY: formatter and field name remain live during each typed SDK call.
        let result = unsafe {
            match value {
                Some(value) => sdk::blpapi_MessageFormatter_setValueChar(
                    self.0.as_ptr(),
                    field.as_ptr(),
                    value as std::ffi::c_char,
                ),
                None => sdk::blpapi_MessageFormatter_setValueNull(self.0.as_ptr(), field.as_ptr()),
            }
        };
        assert_eq!(result, 0, "format test Char");
    }

    pub fn datetime(&mut self, field: &str, value: &sdk::blpapi_HighPrecisionDatetime_t) {
        let field = Name::get_or_intern(field);
        // SAFETY: formatter, field and the initialized datetime remain live for the call.
        assert_eq!(
            unsafe {
                sdk::blpapi_MessageFormatter_setValueHighPrecisionDatetime(
                    self.0.as_ptr(),
                    field.as_ptr(),
                    value,
                )
            },
            0,
            "format test datetime"
        );
    }
}

impl Drop for TestMessageFormatter {
    fn drop(&mut self) {
        // SAFETY: this owns the formatter created by appendMessage.
        unsafe { sdk::blpapi_MessageFormatter_destroy(self.0.as_ptr()) };
    }
}
