//! FFI bindings to Bloomberg BLPAPI
//!
//! Re-exports from xbbg-blpapi-sys plus local type definitions.

// --- Opaque types ---
pub use blpapi_sys::{
    blpapi_AuthApplication_t, blpapi_AuthOptions_t, blpapi_AuthToken_t, blpapi_AuthUser_t,
    blpapi_CorrelationId_t, blpapi_Element_t, blpapi_Event_t, blpapi_Identity_t,
    blpapi_MessageIterator_t, blpapi_Message_t, blpapi_Name_t, blpapi_Request_t, blpapi_Service_t,
    blpapi_SessionOptions_t, blpapi_Session_t, blpapi_SubscriptionList_t, blpapi_TlsOptions_t,
};

// --- Auth functions ---
pub use blpapi_sys::{
    blpapi_AuthApplication_create, blpapi_AuthApplication_destroy,
    blpapi_AuthOptions_create_default, blpapi_AuthOptions_create_forAppMode,
    blpapi_AuthOptions_create_forToken, blpapi_AuthOptions_create_forUserAndAppMode,
    blpapi_AuthOptions_create_forUserMode, blpapi_AuthOptions_destroy, blpapi_AuthToken_create,
    blpapi_AuthToken_destroy, blpapi_AuthUser_createWithActiveDirectoryProperty,
    blpapi_AuthUser_createWithLogonName, blpapi_AuthUser_createWithManualOptions,
    blpapi_AuthUser_destroy,
};

// --- Schema opaque types ---
pub use blpapi_sys::{
    blpapi_ConstantList_t, blpapi_Constant_t, blpapi_Operation_t, blpapi_SchemaElementDefinition_t,
    blpapi_SchemaTypeDefinition_t,
};

// --- Name functions ---
pub use blpapi_sys::{
    blpapi_Name_create, blpapi_Name_destroy, blpapi_Name_duplicate, blpapi_Name_findName,
    blpapi_Name_string,
};

// --- Element functions ---
pub use blpapi_sys::{
    blpapi_Element_datatype, blpapi_Element_getElement, blpapi_Element_getElementAt,
    blpapi_Element_getValueAsBool, blpapi_Element_getValueAsBytes, blpapi_Element_getValueAsChar,
    blpapi_Element_getValueAsElement, blpapi_Element_getValueAsFloat64,
    blpapi_Element_getValueAsInt32, blpapi_Element_getValueAsInt64,
    blpapi_Element_getValueAsString, blpapi_Element_isArray, blpapi_Element_isNull,
    blpapi_Element_name, blpapi_Element_numElements, blpapi_Element_numValues,
};

// --- Element setters ---
pub use blpapi_sys::{
    blpapi_Element_appendElement, blpapi_Element_setElementFloat64, blpapi_Element_setElementInt32,
    blpapi_Element_setElementString, blpapi_Element_setValueFloat64, blpapi_Element_setValueInt32,
    blpapi_Element_setValueInt64, blpapi_Element_setValueString, BLPAPI_ELEMENT_INDEX_END,
};

// --- Message functions ---
pub use blpapi_sys::{
    blpapi_Message_correlationId, blpapi_Message_elements, blpapi_Message_messageType,
    blpapi_Message_numCorrelationIds, blpapi_Message_typeString,
};

// --- Event functions ---
pub use blpapi_sys::{
    blpapi_Event_eventType, blpapi_Event_release, blpapi_MessageIterator_create,
    blpapi_MessageIterator_destroy, blpapi_MessageIterator_next,
};

// --- Session functions ---
pub use blpapi_sys::{
    blpapi_Session_cancel, blpapi_Session_create, blpapi_Session_createIdentity,
    blpapi_Session_destroy, blpapi_Session_generateToken, blpapi_Session_getService,
    blpapi_Session_nextEvent, blpapi_Session_openService, blpapi_Session_openServiceAsync,
    blpapi_Session_resubscribe, blpapi_Session_sendAuthorizationRequest,
    blpapi_Session_sendRequest, blpapi_Session_start, blpapi_Session_stop,
    blpapi_Session_stopAsync, blpapi_Session_subscribe, blpapi_Session_tryNextEvent,
    blpapi_Session_unsubscribe,
};

// --- Service functions ---
pub use blpapi_sys::{blpapi_Service_createRequest, blpapi_Service_name};

// --- Service schema introspection ---
pub use blpapi_sys::{
    blpapi_Service_description, blpapi_Service_getOperationAt, blpapi_Service_numOperations,
};

// --- Operation functions ---
pub use blpapi_sys::{
    blpapi_Operation_description, blpapi_Operation_name, blpapi_Operation_numResponseDefinitions,
    blpapi_Operation_requestDefinition, blpapi_Operation_responseDefinition,
};

// --- SchemaElementDefinition functions ---
pub use blpapi_sys::{
    blpapi_SchemaElementDefinition_description, blpapi_SchemaElementDefinition_maxValues,
    blpapi_SchemaElementDefinition_minValues, blpapi_SchemaElementDefinition_name,
    blpapi_SchemaElementDefinition_type,
};

#[cfg(feature = "live")]
pub use blpapi_sys::blpapi_SchemaElementDefinition_status;

// --- SchemaTypeDefinition functions ---
pub use blpapi_sys::{
    blpapi_SchemaTypeDefinition_datatype, blpapi_SchemaTypeDefinition_description,
    blpapi_SchemaTypeDefinition_enumeration, blpapi_SchemaTypeDefinition_getElementDefinitionAt,
    blpapi_SchemaTypeDefinition_isComplexType, blpapi_SchemaTypeDefinition_isEnumerationType,
    blpapi_SchemaTypeDefinition_isSimpleType, blpapi_SchemaTypeDefinition_name,
    blpapi_SchemaTypeDefinition_numElementDefinitions,
};

#[cfg(feature = "live")]
pub use blpapi_sys::blpapi_SchemaTypeDefinition_status;

// --- ConstantList/Constant functions ---
pub use blpapi_sys::{
    blpapi_ConstantList_getConstantAt, blpapi_ConstantList_numConstants,
    blpapi_Constant_description, blpapi_Constant_name,
};

// --- Request functions ---
pub use blpapi_sys::{blpapi_Request_destroy, blpapi_Request_elements};

// --- SubscriptionList functions ---
pub use blpapi_sys::{
    blpapi_SubscriptionList_add, blpapi_SubscriptionList_create, blpapi_SubscriptionList_destroy,
};

// --- CorrelationId constants ---
pub use blpapi_sys::{
    BLPAPI_CORRELATION_TYPE_AUTOGEN, BLPAPI_CORRELATION_TYPE_INT, BLPAPI_CORRELATION_TYPE_POINTER,
    BLPAPI_CORRELATION_TYPE_UNSET,
};

// --- SessionOptions functions ---
pub use blpapi_sys::{
    blpapi_SessionOptions_create, blpapi_SessionOptions_destroy,
    blpapi_SessionOptions_maxEventQueueSize, blpapi_SessionOptions_setAuthenticationOptions,
    blpapi_SessionOptions_setAutoRestartOnDisconnection,
    blpapi_SessionOptions_setBandwidthSaveModeDisabled, blpapi_SessionOptions_setConnectTimeout,
    blpapi_SessionOptions_setDefaultKeepAliveInactivityTime,
    blpapi_SessionOptions_setDefaultKeepAliveResponseTimeout,
    blpapi_SessionOptions_setDefaultSubscriptionService,
    blpapi_SessionOptions_setDefaultTopicPrefix,
    blpapi_SessionOptions_setFlushPublishedEventsTimeout,
    blpapi_SessionOptions_setKeepAliveEnabled, blpapi_SessionOptions_setMaxEventQueueSize,
    blpapi_SessionOptions_setNumStartAttempts,
    blpapi_SessionOptions_setRecordSubscriptionDataReceiveTimes,
    blpapi_SessionOptions_setServerAddress, blpapi_SessionOptions_setServerHost,
    blpapi_SessionOptions_setServerPort, blpapi_SessionOptions_setServiceCheckTimeout,
    blpapi_SessionOptions_setServiceDownloadTimeout,
    blpapi_SessionOptions_setSessionIdentityOptions,
    blpapi_SessionOptions_setSlowConsumerWarningHiWaterMark,
    blpapi_SessionOptions_setSlowConsumerWarningLoWaterMark, blpapi_SessionOptions_setTlsOptions,
};

// --- TlsOptions functions ---
pub use blpapi_sys::{
    blpapi_TlsOptions_createFromBlobs, blpapi_TlsOptions_createFromFiles,
    blpapi_TlsOptions_destroy, blpapi_TlsOptions_setCrlFetchTimeoutMs,
    blpapi_TlsOptions_setTlsHandshakeTimeoutMs,
};

// --- Socks5Config type ---
pub use blpapi_sys::blpapi_Socks5Config_t;

// --- Socks5Config functions ---
pub use blpapi_sys::{blpapi_Socks5Config_create, blpapi_Socks5Config_destroy};

// --- SessionOptions proxy function ---
pub use blpapi_sys::blpapi_SessionOptions_setServerAddressWithProxy;

// --- Identity functions ---
pub use blpapi_sys::{
    blpapi_AbstractSession_generateAuthorizedIdentityAsync,
    blpapi_AbstractSession_getAuthorizedIdentity, blpapi_AbstractSession_t,
    blpapi_Identity_getSeatType, blpapi_Identity_hasEntitlements, blpapi_Identity_isAuthorized,
    blpapi_Identity_release, blpapi_Session_getAbstractSession,
};

// --- Logging functions ---
pub use blpapi_sys::{
    blpapi_Datetime_tag as blpapi_Logging_Datetime_t, blpapi_Logging_Func_t,
    blpapi_Logging_Severity_t, blpapi_Logging_Severity_t_blpapi_Logging_SEVERITY_DEBUG,
    blpapi_Logging_Severity_t_blpapi_Logging_SEVERITY_ERROR,
    blpapi_Logging_Severity_t_blpapi_Logging_SEVERITY_FATAL,
    blpapi_Logging_Severity_t_blpapi_Logging_SEVERITY_INFO,
    blpapi_Logging_Severity_t_blpapi_Logging_SEVERITY_OFF,
    blpapi_Logging_Severity_t_blpapi_Logging_SEVERITY_TRACE,
    blpapi_Logging_Severity_t_blpapi_Logging_SEVERITY_WARN, blpapi_Logging_logTestMessage,
    blpapi_Logging_registerCallback,
};

// --- ZfpUtil functions ---
pub use blpapi_sys::blpapi_ZfpUtil_getOptionsForLeasedLines;

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

// Declare datetime + timepoint FFI using our local types (not blpapi-sys's)
extern "C" {
    pub fn blpapi_Element_getValueAsHighPrecisionDatetime(
        element: *mut blpapi_Element_t,
        buffer: *mut blpapi_HighPrecisionDatetime_t,
        index: usize,
    ) -> i32;

    pub fn blpapi_Element_setElementDatetime(
        element: *mut blpapi_Element_t,
        name_string: *const std::os::raw::c_char,
        name: *const blpapi_Name_t,
        value: *const blpapi_Datetime_t,
    ) -> i32;

    pub fn blpapi_Element_setValueDatetime(
        element: *mut blpapi_Element_t,
        value: *const blpapi_Datetime_t,
        index: usize,
    ) -> i32;

    /// Get the time this message was received by the SDK.
    ///
    /// Returns 0 on success. Fails if receive-time recording was not enabled
    /// via `blpapi_SessionOptions_setRecordSubscriptionDataReceiveTimes`.
    pub fn blpapi_Message_timeReceived(
        message: *const blpapi_Message_t,
        timeReceived: *mut blpapi_TimePoint_t,
    ) -> i32;

    /// Convert a TimePoint to a HighPrecisionDatetime.
    ///
    /// `offset` is the timezone offset in minutes from UTC (0 = UTC).
    pub fn blpapi_HighPrecisionDatetime_fromTimePoint(
        datetime: *mut blpapi_HighPrecisionDatetime_t,
        timePoint: *const blpapi_TimePoint_t,
        offset: i16,
    ) -> i32;

    pub fn blpapi_getVersionInfo(
        majorVersion: *mut i32,
        minorVersion: *mut i32,
        patchVersion: *mut i32,
        buildVersion: *mut i32,
    );
}
