use std::ffi::CStr;
use std::sync::Once;

static INIT: Once = Once::new();

fn severity_name(severity: i32) -> &'static str {
    match severity {
        1 => "FATAL",
        2 => "ERROR",
        3 => "WARN",
        4 => "INFO",
        5 => "DEBUG",
        6 => "TRACE",
        _ => "UNKNOWN",
    }
}

unsafe extern "C" fn sdk_callback(
    _thread_id: u64,
    severity: i32,
    _timestamp: xbbg_core::ffi::blpapi_Logging_Datetime_t,
    category: *const std::ffi::c_char,
    message: *const std::ffi::c_char,
) {
    let cat = if category.is_null() {
        "sdk"
    } else {
        // SAFETY: non-null was checked above; the SDK passes a NUL-terminated
        // category string that stays valid for the duration of this callback.
        unsafe { CStr::from_ptr(category) }
            .to_str()
            .unwrap_or("sdk")
    };
    let msg = if message.is_null() {
        ""
    } else {
        // SAFETY: non-null was checked above; the SDK passes a NUL-terminated
        // message string that stays valid for the duration of this callback.
        unsafe { CStr::from_ptr(message) }.to_str().unwrap_or("")
    };

    match severity {
        1 => {
            xbbg_log::error!(target: "xbbg.sdk", category = cat, "[{}] {}", severity_name(severity), msg)
        }
        2 => {
            xbbg_log::error!(target: "xbbg.sdk", category = cat, "[{}] {}", severity_name(severity), msg)
        }
        3 => {
            xbbg_log::warn!(target: "xbbg.sdk", category = cat, "[{}] {}", severity_name(severity), msg)
        }
        4 => {
            xbbg_log::info!(target: "xbbg.sdk", category = cat, "[{}] {}", severity_name(severity), msg)
        }
        5 => {
            xbbg_log::debug!(target: "xbbg.sdk", category = cat, "[{}] {}", severity_name(severity), msg)
        }
        _ => {
            xbbg_log::trace!(target: "xbbg.sdk", category = cat, "[{}] {}", severity_name(severity), msg)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SdkLogLevel {
    Off,
    Fatal,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl SdkLogLevel {
    fn to_raw(self) -> i32 {
        match self {
            Self::Off => 0,
            Self::Fatal => 1,
            Self::Error => 2,
            Self::Warn => 3,
            Self::Info => 4,
            Self::Debug => 5,
            Self::Trace => 6,
        }
    }
}

impl std::str::FromStr for SdkLogLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "fatal" => Ok(Self::Fatal),
            "error" => Ok(Self::Error),
            "warn" | "warning" => Ok(Self::Warn),
            "info" => Ok(Self::Info),
            "debug" => Ok(Self::Debug),
            "trace" => Ok(Self::Trace),
            _ => Err(format!(
                "invalid SDK log level: {s} (expected off/fatal/error/warn/info/debug/trace)"
            )),
        }
    }
}

pub fn register_sdk_logging(level: SdkLogLevel) {
    if level == SdkLogLevel::Off {
        return;
    }

    INIT.call_once(|| {
        let rc = unsafe {
            xbbg_core::ffi::blpapi_Logging_registerCallback(
                Some(sdk_callback),
                level.to_raw() as xbbg_core::ffi::blpapi_Logging_Severity_t,
            )
        };
        if rc != 0 {
            xbbg_log::warn!("failed to register Bloomberg SDK logging callback: rc={rc}");
        } else {
            xbbg_log::info!(level = ?level, "Bloomberg SDK logging callback registered");
        }
    });
}
