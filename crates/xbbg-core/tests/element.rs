//! Scalar extraction checks using SDK fixtures, without a Bloomberg session.

#![cfg(feature = "test-support")]

use xbbg_core::test_support::TestEvent;
use xbbg_core::{ffi, Value};

#[test]
fn checked_and_cached_dispatch_preserve_scalar_values() {
    let schema = r#"<ServiceDefinition name="xbbg.test.scalars" version="1.0.0.0">
        <service name="//xbbg/test/scalars" version="1.0.0.0">
            <event name="Scalars" eventType="ScalarsType"/>
        </service>
        <schema><sequenceType name="ScalarsType">
            <element name="INTEGER" type="Int32"/>
            <element name="NUMBER" type="Float64"/>
            <element name="TEXT" type="String"/>
            <element name="DAY" type="Date"/>
            <element name="CLOCK" type="Time"/>
            <element name="STAMP" type="Datetime"/>
            <element name="MISSING" type="Float64" minOccurs="0"/>
        </sequenceType></schema>
    </ServiceDefinition>"#;
    let event = TestEvent::subscription(schema, "Scalars", |formatter| {
        formatter.json(r#"{"INTEGER":7,"NUMBER":1.25,"TEXT":"value","DAY":"1970-01-02","CLOCK":"01:02:03","STAMP":"1970-01-02T00:00:01Z","MISSING":null}"#);
    });
    let mut messages = event.event().messages();
    let message = messages.next().unwrap();
    let root = message.elements();

    for (field, expected) in [
        ("INTEGER", Value::Int32(7)),
        ("NUMBER", Value::Float64(1.25)),
        ("TEXT", Value::String("value")),
        ("DAY", Value::Date32(1)),
        ("CLOCK", Value::Time64Micros(3_723_000_000)),
        ("STAMP", Value::TimestampMicros(86_401_000_000)),
    ] {
        let element = root.get_by_str(field).unwrap();
        assert_eq!(element.get_value(0), Some(expected.clone()), "{field}");
        assert_eq!(
            element.get_value_fast_with_datatype(0, element.datatype()),
            Some(expected),
            "{field}"
        );
        assert_eq!(element.get_value(element.len()), None, "{field}");
    }

    let missing = root.get_by_str("MISSING").unwrap();
    assert_eq!(missing.get_value(0), Some(Value::Null));
    // The checked API deliberately tests element nullness before index bounds.
    assert_eq!(missing.get_value(usize::MAX), Some(Value::Null));
}

#[test]
fn checked_and_cached_dispatch_preserve_char_coercion() {
    let schema = r#"<ServiceDefinition name="xbbg.test.characters" version="1.0.0.0">
        <service name="//xbbg/test/characters" version="1.0.0.0">
            <event name="Characters" eventType="CharactersType"/>
        </service>
        <schema><sequenceType name="CharactersType">
            <element name="FLAG" type="Char"/>
        </sequenceType></schema>
    </ServiceDefinition>"#;
    for (character, expected) in [
        (b'Y', Value::Bool(true)),
        (b'N', Value::Bool(false)),
        (b'X', Value::Byte(b'X')),
    ] {
        let event = TestEvent::subscription(schema, "Characters", |formatter| {
            formatter.char("FLAG", Some(character));
        });
        let mut messages = event.event().messages();
        let message = messages.next().unwrap();
        let element = message.elements().get_by_str("FLAG").unwrap();
        assert_eq!(element.get_value(0), Some(expected.clone()));
        assert_eq!(
            element.get_value_fast_with_datatype(0, element.datatype()),
            Some(expected)
        );
    }
}

#[test]
fn datetime_without_date_parts_remains_time_only() {
    let schema = r#"<ServiceDefinition name="xbbg.test.time_only" version="1.0.0.0">
        <service name="//xbbg/test/time_only" version="1.0.0.0">
            <event name="TimeOnly" eventType="TimeOnlyType"/>
        </service>
        <schema><sequenceType name="TimeOnlyType">
            <element name="STAMP" type="Datetime"/>
        </sequenceType></schema>
    </ServiceDefinition>"#;
    let datetime = ffi::SdkHighPrecisionDatetime {
        datetime: ffi::SdkDatetime {
            parts: 112, // Hour/minute/second, without date or millisecond parts.
            hours: 1,
            minutes: 2,
            seconds: 3,
            milliSeconds: 0,
            month: 0,
            day: 0,
            year: 0,
            offset: 0,
        },
        picoseconds: 0,
    };
    let event = TestEvent::subscription(schema, "TimeOnly", |formatter| {
        formatter.datetime("STAMP", &datetime);
    });
    let mut messages = event.event().messages();
    let message = messages.next().unwrap();
    let element = message.elements().get_by_str("STAMP").unwrap();
    assert_eq!(
        element.get_value(0),
        Some(Value::Time64Micros(3_723_000_000))
    );
    assert_eq!(
        element.get_value_fast_with_datatype(0, element.datatype()),
        Some(Value::Time64Micros(3_723_000_000))
    );
}
