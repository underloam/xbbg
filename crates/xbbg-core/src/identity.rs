//! Identity for authenticated Bloomberg sessions

use crate::errors::{BlpError, Result};
use crate::sdk;

/// Identity handle for authenticated Bloomberg sessions.
///
/// Identities are created by the session and used for authorization.
/// Required for accessing permissioned data or services.
///
/// # Examples
///
/// ```ignore
/// // Generate token and authorize
/// let identity = session.generate_token()?;
/// session.authorize(&identity)?;
///
/// // Use identity for permissioned requests
/// session.send_request(&req, Some(&identity), None)?;
/// ```
///
/// # Lifecycle
/// The identity is a reference-counted Bloomberg handle. The local SDK headers
/// do not document cross-thread use, so this wrapper is not `Send` or `Sync`.
pub struct Identity {
    ptr: *mut crate::ffi::blpapi_Identity_t,
}

impl Identity {
    /// Create an Identity from a raw pointer (internal use only)
    pub(crate) fn from_raw(ptr: *mut crate::ffi::blpapi_Identity_t) -> Result<Self> {
        if ptr.is_null() {
            return Err(BlpError::Internal {
                detail: "null identity pointer".into(),
            });
        }
        Ok(Self { ptr })
    }

    pub(crate) fn as_ptr(&self) -> *mut crate::ffi::blpapi_Identity_t {
        self.ptr
    }

    pub fn is_authorized(&self, service: &crate::Service<'_>) -> bool {
        let rc = unsafe { sdk().blpapi_Identity_isAuthorized(self.ptr, service.as_ptr()) };
        rc != 0
    }

    /// Return whether this identity is entitled to ALL of the given
    /// entitlement IDs for `service`.
    ///
    /// Mirrors the C++ `Identity::hasEntitlements` contract: the FFI return
    /// value is the boolean answer (nonzero = entitled), not an error code.
    /// The `failedEntitlements` out-array is optional and we don't request it;
    /// its count parameter is in/out (input = array capacity), so it must be 0
    /// when no array is supplied.
    pub fn has_entitlements(&self, service: &crate::Service<'_>, eids: &[i32]) -> Result<bool> {
        let mut failed_count: i32 = 0;
        let rc = unsafe {
            sdk().blpapi_Identity_hasEntitlements(
                self.ptr,
                service.as_ptr(),
                std::ptr::null(),
                eids.as_ptr(),
                eids.len(),
                std::ptr::null_mut(),
                &mut failed_count,
            )
        };
        Ok(rc != 0)
    }

    /// Check entitlements for `service`, reporting which EIDs failed.
    ///
    /// Same FFI contract as [`Identity::has_entitlements`], but supplies the
    /// optional `failedEntitlements` out-array (capacity = `eids.len()`; the
    /// count parameter is in/out: capacity on input, failures written on
    /// output — blpapi_identity.h:196-216). When the identity is entitled to
    /// every EID the array is untouched and `failed_eids` is empty.
    pub fn check_entitlements(
        &self,
        service: &crate::Service<'_>,
        eids: &[i32],
    ) -> Result<EntitlementCheck> {
        let mut failed = vec![0_i32; eids.len()];
        let mut failed_count: i32 = eids.len() as i32;
        let rc = unsafe {
            sdk().blpapi_Identity_hasEntitlements(
                self.ptr,
                service.as_ptr(),
                std::ptr::null(),
                eids.as_ptr(),
                eids.len(),
                failed.as_mut_ptr(),
                &mut failed_count,
            )
        };
        let entitled = rc != 0;
        let failed_eids = if entitled {
            Vec::new()
        } else {
            let count = usize::try_from(failed_count).unwrap_or(0).min(failed.len());
            failed.truncate(count);
            failed
        };
        Ok(EntitlementCheck {
            entitled,
            failed_eids,
        })
    }

    pub fn seat_type(&self) -> Result<SeatType> {
        let mut raw: i32 = -1;
        let rc = unsafe { sdk().blpapi_Identity_getSeatType(self.ptr, &mut raw) };
        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("getSeatType failed: rc={rc}"),
            });
        }
        Ok(SeatType::from_raw(raw))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeatType {
    Bps,
    NonBps,
    Invalid,
}

impl SeatType {
    fn from_raw(raw: i32) -> Self {
        match raw {
            0 => Self::Bps,
            1 => Self::NonBps,
            _ => Self::Invalid,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bps => "BPS",
            Self::NonBps => "NONBPS",
            Self::Invalid => "INVALID",
        }
    }
}

/// Result of an entitlement check against a service.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EntitlementCheck {
    /// Whether the identity is entitled to every requested EID.
    pub entitled: bool,
    /// The subset of requested EIDs the identity is NOT entitled to.
    /// Empty when `entitled` is true.
    pub failed_eids: Vec<i32>,
}

impl Drop for Identity {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: `blpapi_Session_createIdentity` hands the caller an owned
            // reference (the C++ `Identity(handle)` ctor takes ownership without
            // addRef and its destructor calls release; blpapi_identity.h:279-290).
            // Releasing exactly once here balances that reference.
            unsafe { sdk().blpapi_Identity_release(self.ptr) };
            self.ptr = std::ptr::null_mut();
        }
    }
}
