use std::ffi::CString;

use crate::errors::{BlpError, Result};
use crate::ffi;
use crate::sdk;

/// Safe wrapper around `blpapi_Socks5Config_t`.
pub struct Socks5Config {
    ptr: *mut ffi::blpapi_Socks5Config_t,
}

impl Socks5Config {
    pub fn new(hostname: &str, port: u16) -> Result<Self> {
        let cs = CString::new(hostname).map_err(|e| BlpError::InvalidArgument {
            detail: format!("invalid socks5 hostname: {e}"),
        })?;
        let ptr =
            unsafe { sdk().blpapi_Socks5Config_create(cs.as_ptr(), cs.as_bytes().len(), port) };
        if ptr.is_null() {
            return Err(BlpError::Internal {
                detail: "blpapi_Socks5Config_create returned null".into(),
            });
        }
        Ok(Self { ptr })
    }

    pub(crate) fn as_ptr(&self) -> *const ffi::blpapi_Socks5Config_t {
        self.ptr
    }
}

impl Drop for Socks5Config {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { sdk().blpapi_Socks5Config_destroy(self.ptr) };
            self.ptr = std::ptr::null_mut();
        }
    }
}
