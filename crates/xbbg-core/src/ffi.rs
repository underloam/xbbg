//! FFI types for Bloomberg BLPAPI
//!
//! Re-exports the opaque types and constants from xbbg-blpapi-sys, plus the datetime types defined
//! locally. There are no function re-exports: the runtime is loaded by path, and every BLPAPI call
//! is `sdk().blpapi_…(…)` on the function table ([`Blpapi`]) that [`crate::sdk`] returns.

// --- The function table ---
pub use blpapi_sys::Blpapi;

// --- Opaque types ---
pub use blpapi_sys::{
    blpapi_AuthApplication_t, blpapi_AuthOptions_t, blpapi_AuthToken_t, blpapi_AuthUser_t,
    blpapi_CorrelationId_t, blpapi_Element_t, blpapi_Event_t, blpapi_Identity_t, blpapi_Message_t,
    blpapi_MessageIterator_t, blpapi_Name_t, blpapi_Request_t, blpapi_Service_t, blpapi_Session_t,
    blpapi_SessionOptions_t, blpapi_SubscriptionList_t, blpapi_TlsOptions_t,
};

// SDK-generated layouts required by the test message formatter. The core
// datetime wrappers below intentionally expose a different Rust field layout.
#[cfg(feature = "test-support")]
pub use blpapi_sys::{
    blpapi_Datetime_t as SdkDatetime, blpapi_HighPrecisionDatetime_t as SdkHighPrecisionDatetime,
};

// --- Schema opaque types ---
pub use blpapi_sys::{
    blpapi_Constant_t, blpapi_ConstantList_t, blpapi_Operation_t, blpapi_SchemaElementDefinition_t,
    blpapi_SchemaTypeDefinition_t,
};

// --- Element setter constants ---
pub use blpapi_sys::BLPAPI_ELEMENT_INDEX_END;

// --- CorrelationId constants ---
pub use blpapi_sys::{
    BLPAPI_CORRELATION_TYPE_AUTOGEN, BLPAPI_CORRELATION_TYPE_INT, BLPAPI_CORRELATION_TYPE_POINTER,
    BLPAPI_CORRELATION_TYPE_UNSET,
};

// --- Socks5Config type ---
pub use blpapi_sys::blpapi_Socks5Config_t;

// --- Identity types ---
pub use blpapi_sys::blpapi_AbstractSession_t;

// --- Logging types ---
pub use blpapi_sys::{
    blpapi_Datetime_tag as blpapi_Logging_Datetime_t, blpapi_Logging_Func_t,
    blpapi_Logging_Severity_t, blpapi_Logging_Severity_t_blpapi_Logging_SEVERITY_DEBUG,
    blpapi_Logging_Severity_t_blpapi_Logging_SEVERITY_ERROR,
    blpapi_Logging_Severity_t_blpapi_Logging_SEVERITY_FATAL,
    blpapi_Logging_Severity_t_blpapi_Logging_SEVERITY_INFO,
    blpapi_Logging_Severity_t_blpapi_Logging_SEVERITY_OFF,
    blpapi_Logging_Severity_t_blpapi_Logging_SEVERITY_TRACE,
    blpapi_Logging_Severity_t_blpapi_Logging_SEVERITY_WARN,
};

// --- HighPrecisionDatetime (defined locally for layout control) ---

/// Bloomberg high-precision datetime structure.
///
/// This is ALWAYS defined locally (not re-exported from blpapi-sys) to guarantee
/// exact layout control. 16 bytes with natural C alignment.
///
/// # Layout
/// Matches the C struct from blpapi_datetime.h (no packing pragma):
/// - blpapi_Datetime_t (12 bytes): parts, hours, minutes, seconds, milliseconds, month, day, year, offset
/// - picoseconds (4 bytes, aligned to 4)
///
/// The fields happen to be naturally aligned, so #[repr(C)] produces identical
/// layout to the C struct without the UB risks of #[repr(C, packed)].
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct blpapi_HighPrecisionDatetime_t {
    pub parts: u8,
    pub hours: u8,
    pub minutes: u8,
    pub seconds: u8,
    pub milliseconds: u16,
    pub month: u8,
    pub day: u8,
    pub year: u16,
    pub offset: i16,
    pub picoseconds: u32,
}

// Compile-time layout verification matching Bloomberg C header
const _: () = {
    assert!(std::mem::size_of::<blpapi_HighPrecisionDatetime_t>() == 16);
    // Verify field offsets match C struct layout
    // parts(0) hours(1) minutes(2) seconds(3) milliseconds(4-5) month(6) day(7) year(8-9) offset(10-11) picoseconds(12-15)
};

/// Bloomberg datetime structure (12 bytes).
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Default)]
pub struct blpapi_Datetime_t {
    pub parts: u8,
    pub hours: u8,
    pub minutes: u8,
    pub seconds: u8,
    pub milliseconds: u16,
    pub month: u8,
    pub day: u8,
    pub year: u16,
    pub offset: i16,
}

// Datetime parts bitmask constants
pub const BLPAPI_DATETIME_YEAR_PART: u8 = 0x01;
pub const BLPAPI_DATETIME_MONTH_PART: u8 = 0x02;
pub const BLPAPI_DATETIME_DAY_PART: u8 = 0x04;
pub const BLPAPI_DATETIME_OFFSET_PART: u8 = 0x08;
pub const BLPAPI_DATETIME_HOURS_PART: u8 = 0x10;
pub const BLPAPI_DATETIME_MINUTES_PART: u8 = 0x20;
pub const BLPAPI_DATETIME_SECONDS_PART: u8 = 0x40;
pub const BLPAPI_DATETIME_MILLISECONDS_PART: u8 = 0x80;
pub const BLPAPI_DATETIME_DATE_PART: u8 =
    BLPAPI_DATETIME_YEAR_PART | BLPAPI_DATETIME_MONTH_PART | BLPAPI_DATETIME_DAY_PART;
pub const BLPAPI_DATETIME_TIME_PART: u8 = BLPAPI_DATETIME_HOURS_PART
    | BLPAPI_DATETIME_MINUTES_PART
    | BLPAPI_DATETIME_SECONDS_PART
    | BLPAPI_DATETIME_MILLISECONDS_PART;

// Compile-time layout verification
const _: () = {
    assert!(std::mem::size_of::<blpapi_Datetime_t>() == 12);
};

// --- TimePoint (for message receive timestamps) ---

/// Bloomberg TimePoint — nanoseconds from an unspecified epoch.
///
/// Used by `blpapi_Message_timeReceived` to record when the SDK received a message.
/// Convert to calendar time via `blpapi_HighPrecisionDatetime_fromTimePoint`.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct blpapi_TimePoint_t {
    pub d_value: i64,
}

// --- The local datetime types against the SDK's ---
//
// `blpapi_Element_getValueAsHighPrecisionDatetime`, `blpapi_Element_setElementDatetime`,
// `blpapi_Message_timeReceived` and `blpapi_HighPrecisionDatetime_fromTimePoint` take the SDK's
// own `blpapi_HighPrecisionDatetime_t`, `blpapi_Datetime_t` and `blpapi_TimePoint_t`, which
// `blpapi-sys` generates with different Rust field names (`milliSeconds`, a nested `datetime`).
// The call sites hand them pointers to the local types above instead, cast with `.cast()`. That is
// sound exactly when both sides are the same C layout, which this block proves at compile time:
// equal size, equal alignment and every field at the same offset.
const _: () = {
    use std::mem::{align_of, offset_of, size_of};

    type SdkDatetime = blpapi_sys::blpapi_Datetime_t;
    type SdkHighPrecisionDatetime = blpapi_sys::blpapi_HighPrecisionDatetime_t;
    type SdkTimePoint = blpapi_sys::blpapi_TimePoint_t;

    assert!(size_of::<blpapi_Datetime_t>() == size_of::<SdkDatetime>());
    assert!(align_of::<blpapi_Datetime_t>() == align_of::<SdkDatetime>());
    assert!(offset_of!(blpapi_Datetime_t, parts) == offset_of!(SdkDatetime, parts));
    assert!(offset_of!(blpapi_Datetime_t, hours) == offset_of!(SdkDatetime, hours));
    assert!(offset_of!(blpapi_Datetime_t, minutes) == offset_of!(SdkDatetime, minutes));
    assert!(offset_of!(blpapi_Datetime_t, seconds) == offset_of!(SdkDatetime, seconds));
    assert!(offset_of!(blpapi_Datetime_t, milliseconds) == offset_of!(SdkDatetime, milliSeconds));
    assert!(offset_of!(blpapi_Datetime_t, month) == offset_of!(SdkDatetime, month));
    assert!(offset_of!(blpapi_Datetime_t, day) == offset_of!(SdkDatetime, day));
    assert!(offset_of!(blpapi_Datetime_t, year) == offset_of!(SdkDatetime, year));
    assert!(offset_of!(blpapi_Datetime_t, offset) == offset_of!(SdkDatetime, offset));

    assert!(size_of::<blpapi_HighPrecisionDatetime_t>() == size_of::<SdkHighPrecisionDatetime>());
    assert!(align_of::<blpapi_HighPrecisionDatetime_t>() == align_of::<SdkHighPrecisionDatetime>());
    assert!(
        offset_of!(blpapi_HighPrecisionDatetime_t, parts)
            == offset_of!(SdkHighPrecisionDatetime, datetime.parts)
    );
    assert!(
        offset_of!(blpapi_HighPrecisionDatetime_t, hours)
            == offset_of!(SdkHighPrecisionDatetime, datetime.hours)
    );
    assert!(
        offset_of!(blpapi_HighPrecisionDatetime_t, minutes)
            == offset_of!(SdkHighPrecisionDatetime, datetime.minutes)
    );
    assert!(
        offset_of!(blpapi_HighPrecisionDatetime_t, seconds)
            == offset_of!(SdkHighPrecisionDatetime, datetime.seconds)
    );
    assert!(
        offset_of!(blpapi_HighPrecisionDatetime_t, milliseconds)
            == offset_of!(SdkHighPrecisionDatetime, datetime.milliSeconds)
    );
    assert!(
        offset_of!(blpapi_HighPrecisionDatetime_t, month)
            == offset_of!(SdkHighPrecisionDatetime, datetime.month)
    );
    assert!(
        offset_of!(blpapi_HighPrecisionDatetime_t, day)
            == offset_of!(SdkHighPrecisionDatetime, datetime.day)
    );
    assert!(
        offset_of!(blpapi_HighPrecisionDatetime_t, year)
            == offset_of!(SdkHighPrecisionDatetime, datetime.year)
    );
    assert!(
        offset_of!(blpapi_HighPrecisionDatetime_t, offset)
            == offset_of!(SdkHighPrecisionDatetime, datetime.offset)
    );
    assert!(
        offset_of!(blpapi_HighPrecisionDatetime_t, picoseconds)
            == offset_of!(SdkHighPrecisionDatetime, picoseconds)
    );

    assert!(size_of::<blpapi_TimePoint_t>() == size_of::<SdkTimePoint>());
    assert!(align_of::<blpapi_TimePoint_t>() == align_of::<SdkTimePoint>());
    assert!(offset_of!(blpapi_TimePoint_t, d_value) == offset_of!(SdkTimePoint, d_value));
};
