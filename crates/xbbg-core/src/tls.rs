use std::ffi::CString;

use crate::errors::{BlpError, Result};
use crate::ffi;

pub struct TlsOptions {
    ptr: *mut ffi::blpapi_TlsOptions_t,
}

impl TlsOptions {
    pub fn from_files(
        client_credentials_path: &str,
        client_credentials_password: &str,
        trust_material_path: &str,
    ) -> Result<Self> {
        let creds = cstring(client_credentials_path, "client credentials path")?;
        let password = cstring(client_credentials_password, "client credentials password")?;
        let trust = cstring(trust_material_path, "trust material path")?;

        let ptr = unsafe {
            ffi::blpapi_TlsOptions_createFromFiles(
                creds.as_ptr(),
                password.as_ptr(),
                trust.as_ptr(),
            )
        };

        if ptr.is_null() {
            return Err(BlpError::Internal {
                detail: "failed to create TlsOptions from files".into(),
            });
        }

        Ok(Self { ptr })
    }

    /// Build TLS options directly from DER PKCS#12 credentials and PKCS#7 trust
    /// material, rejecting lengths that cannot be represented by the SDK.
    pub fn from_blobs(
        client_credentials: &[u8],
        client_credentials_password: &str,
        trust_material: &[u8],
    ) -> Result<Self> {
        let credentials_len = blob_len(client_credentials.len(), "client credentials")?;
        let trust_len = blob_len(trust_material.len(), "trust material")?;
        let password = cstring(client_credentials_password, "client credentials password")?;

        let ptr = unsafe {
            ffi::blpapi_TlsOptions_createFromBlobs(
                client_credentials.as_ptr() as *const i8,
                credentials_len,
                password.as_ptr(),
                trust_material.as_ptr() as *const i8,
                trust_len,
            )
        };

        if ptr.is_null() {
            return Err(BlpError::Internal {
                detail: "failed to create TlsOptions from blobs".into(),
            });
        }

        Ok(Self { ptr })
    }

    pub fn set_tls_handshake_timeout_ms(&mut self, timeout_ms: i32) {
        unsafe { ffi::blpapi_TlsOptions_setTlsHandshakeTimeoutMs(self.ptr, timeout_ms) };
    }

    pub fn set_crl_fetch_timeout_ms(&mut self, timeout_ms: i32) {
        unsafe { ffi::blpapi_TlsOptions_setCrlFetchTimeoutMs(self.ptr, timeout_ms) };
    }

    pub(crate) fn as_ptr(&self) -> *const ffi::blpapi_TlsOptions_t {
        self.ptr
    }
}

impl Drop for TlsOptions {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { ffi::blpapi_TlsOptions_destroy(self.ptr) };
            self.ptr = std::ptr::null_mut();
        }
    }
}

fn blob_len(len: usize, field: &str) -> Result<i32> {
    i32::try_from(len).map_err(|_| BlpError::InvalidArgument {
        detail: format!("{field} blob exceeds the SDK maximum length"),
    })
}

fn cstring(value: &str, field: &str) -> Result<CString> {
    CString::new(value).map_err(|_| BlpError::InvalidArgument {
        detail: format!("invalid {field}: contains a NUL byte"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_lengths_fit_sdk_signed_integer() {
        for len in [0, 1, i32::MAX as usize] {
            assert_eq!(blob_len(len, "synthetic").unwrap(), len as i32);
        }
        for len in [i32::MAX as usize + 1, usize::MAX] {
            assert!(matches!(
                blob_len(len, "synthetic"),
                Err(BlpError::InvalidArgument { .. })
            ));
        }
    }

    #[test]
    fn password_validation_does_not_disclose_secret() {
        let password = "synthetic-before\0synthetic-after";
        let error = cstring(password, "client credentials password").unwrap_err();
        for rendered in [error.to_string(), format!("{error:?}")] {
            assert!(!rendered.contains("synthetic-before"));
            assert!(!rendered.contains("synthetic-after"));
        }
    }
}
