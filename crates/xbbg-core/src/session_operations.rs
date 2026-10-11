//! Shared FFI marshalling for synchronous and callback-driven sessions.
//!
//! Session owners retain their lifecycle and threading contracts. This borrowed
//! handle cannot outlive its owner, including any service views it creates.

use std::ffi::{CString, c_char};

use crate::{BlpError, CorrelationId, Identity, Request, Result, Service, SubscriptionList, ffi};

pub(crate) struct SessionOperations<'session> {
    ptr: &'session *mut ffi::blpapi_Session_t,
}

impl<'session> SessionOperations<'session> {
    /// # Safety
    /// `ptr` must refer to a valid session for the entire borrow. The caller must
    /// uphold that session's threading contract for every operation.
    pub(crate) unsafe fn new(ptr: &'session *mut ffi::blpapi_Session_t) -> Self {
        Self { ptr }
    }

    pub(crate) fn open_service(&self, name: &str) -> Result<()> {
        let c_name = service_name(name)?;
        // SAFETY: the owner guarantees a live handle; c_name survives the call.
        let rc = unsafe { ffi::blpapi_Session_openService(*self.ptr, c_name.as_ptr()) };
        check_service_result(rc, name)
    }

    pub(crate) fn open_service_async(
        &self,
        name: &str,
        cid: &CorrelationId,
    ) -> Result<CorrelationId> {
        let c_name = service_name(name)?;
        let mut cid_ffi = cid.to_ffi();
        // SAFETY: valid session/name pointers and initialized CID out-parameter.
        let rc = unsafe {
            ffi::blpapi_Session_openServiceAsync(*self.ptr, c_name.as_ptr(), &mut cid_ffi)
        };
        check_service_result(rc, name)?;
        Ok(CorrelationId::from_ffi(&cid_ffi))
    }

    pub(crate) fn get_service(&self, name: &str) -> Result<Service<'session>> {
        let c_name = service_name(name)?;
        let mut service_ptr = std::ptr::null_mut();
        // SAFETY: valid session/name pointers and service out-parameter. The
        // returned Service borrows the same owner as this operations handle.
        let rc =
            unsafe { ffi::blpapi_Session_getService(*self.ptr, &mut service_ptr, c_name.as_ptr()) };
        check_service_result(rc, name)?;
        Service::from_raw(service_ptr)
    }

    pub(crate) fn send_request(
        &self,
        request: &Request,
        identity: Option<&Identity>,
        cid: Option<&CorrelationId>,
        label: Option<&str>,
    ) -> Result<CorrelationId> {
        let mut cid_ffi = cid.unwrap_or(&CorrelationId::Unset).to_ffi();
        let identity_ptr = identity.map_or(std::ptr::null_mut(), Identity::as_ptr);
        let label = RequestLabel::new(label, "request label")?;
        // SAFETY: borrowed handles and label storage survive the call. A null
        // event queue uses the owning session's event delivery mode.
        let rc = unsafe {
            ffi::blpapi_Session_sendRequest(
                *self.ptr,
                request.as_ptr(),
                &mut cid_ffi,
                identity_ptr,
                std::ptr::null_mut(),
                label.as_ptr(),
                label.len(),
            )
        };
        check_result(rc, "blpapi_Session_sendRequest")?;
        Ok(CorrelationId::from_ffi(&cid_ffi))
    }

    pub(crate) fn subscribe(
        &self,
        subs: &SubscriptionList,
        identity: Option<&Identity>,
        label: Option<&str>,
        label_kind: &str,
    ) -> Result<()> {
        let label = RequestLabel::new(label, label_kind)?;
        let identity_ptr = identity.map_or(std::ptr::null_mut(), Identity::as_ptr);
        // SAFETY: valid borrowed handles; the optional label survives the call.
        let rc = unsafe {
            ffi::blpapi_Session_subscribe(
                *self.ptr,
                subs.as_ptr(),
                identity_ptr,
                label.as_ptr(),
                label.len(),
            )
        };
        let operation = if identity.is_some() {
            "blpapi_Session_subscribe (with identity)"
        } else {
            "blpapi_Session_subscribe"
        };
        check_result(rc, operation)
    }

    pub(crate) fn resubscribe(
        &self,
        subs: &SubscriptionList,
        label: Option<&str>,
        label_kind: &str,
    ) -> Result<()> {
        let label = RequestLabel::new(label, label_kind)?;
        // SAFETY: valid borrowed handles; the optional label survives the call.
        let rc = unsafe {
            ffi::blpapi_Session_resubscribe(*self.ptr, subs.as_ptr(), label.as_ptr(), label.len())
        };
        check_result(rc, "blpapi_Session_resubscribe")
    }

    pub(crate) fn unsubscribe(&self, subs: &SubscriptionList) -> Result<()> {
        // SAFETY: valid session/list pointers; no diagnostic label is supplied.
        let rc = unsafe {
            ffi::blpapi_Session_unsubscribe(*self.ptr, subs.as_ptr(), std::ptr::null(), 0)
        };
        check_result(rc, "blpapi_Session_unsubscribe")
    }

    pub(crate) fn cancel(&self, cid: &CorrelationId) -> Result<()> {
        let cid_ffi = cid.to_ffi();
        // SAFETY: valid session pointer and one initialized CID by pointer/count.
        let rc = unsafe { ffi::blpapi_Session_cancel(*self.ptr, &cid_ffi, 1, std::ptr::null(), 0) };
        check_result(rc, "blpapi_Session_cancel")
    }

    pub(crate) fn create_identity(&self) -> Result<Identity> {
        // SAFETY: valid session pointer. Identity owns the returned reference
        // and releases it exactly once on drop.
        let identity_ptr = unsafe { ffi::blpapi_Session_createIdentity(*self.ptr) };
        Identity::from_raw(identity_ptr)
    }

    pub(crate) fn generate_token(&self, cid: Option<&CorrelationId>) -> Result<CorrelationId> {
        let mut cid_ffi = cid.unwrap_or(&CorrelationId::Unset).to_ffi();
        // SAFETY: valid session and CID pointers; a null event queue uses the
        // owning session's event delivery mode.
        let rc = unsafe {
            ffi::blpapi_Session_generateToken(*self.ptr, &mut cid_ffi, std::ptr::null_mut())
        };
        check_result(rc, "blpapi_Session_generateToken")?;
        Ok(CorrelationId::from_ffi(&cid_ffi))
    }

    pub(crate) fn send_authorization_request(
        &self,
        request: &Request,
        identity: &mut Identity,
        cid: Option<&CorrelationId>,
    ) -> Result<CorrelationId> {
        let mut cid_ffi = cid.unwrap_or(&CorrelationId::Unset).to_ffi();
        // SAFETY: valid borrowed handles and CID out-parameter; the null event
        // queue routes authorization events through the owning session.
        let rc = unsafe {
            ffi::blpapi_Session_sendAuthorizationRequest(
                *self.ptr,
                request.as_ptr(),
                identity.as_ptr(),
                &mut cid_ffi,
                std::ptr::null_mut(),
                std::ptr::null(),
                0,
            )
        };
        check_result(rc, "blpapi_Session_sendAuthorizationRequest")?;
        Ok(CorrelationId::from_ffi(&cid_ffi))
    }
}

fn service_name(name: &str) -> Result<CString> {
    CString::new(name).map_err(|e| BlpError::InvalidArgument {
        detail: format!("invalid service name: {e}"),
    })
}

fn check_service_result(rc: i32, name: &str) -> Result<()> {
    if rc != 0 {
        return Err(BlpError::OpenService {
            service: name.to_owned(),
            source: None,
            label: None,
        });
    }
    Ok(())
}

fn check_result(rc: i32, operation: &str) -> Result<()> {
    if rc != 0 {
        return Err(BlpError::Internal {
            detail: format!("{operation} failed with rc={rc}"),
        });
    }
    Ok(())
}

struct RequestLabel(Option<CString>);

impl RequestLabel {
    fn new(value: Option<&str>, kind: &str) -> Result<Self> {
        value
            .map(CString::new)
            .transpose()
            .map(Self)
            .map_err(|e| BlpError::InvalidArgument {
                detail: format!("invalid {kind}: {e}"),
            })
    }

    fn as_ptr(&self) -> *const c_char {
        self.0.as_ref().map_or(std::ptr::null(), |s| s.as_ptr())
    }

    fn len(&self) -> i32 {
        self.0.as_ref().map_or(0, |s| s.as_bytes().len() as i32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_preserve_absence_and_utf8_byte_length() {
        let absent = RequestLabel::new(None, "label").unwrap();
        assert!(absent.as_ptr().is_null());
        assert_eq!(absent.len(), 0);

        for value in ["", "request", "\u{00e9}"] {
            let label = RequestLabel::new(Some(value), "label").unwrap();
            assert!(!label.as_ptr().is_null());
            assert_eq!(label.len(), value.len() as i32);
            assert_eq!(label.0.as_ref().unwrap().to_bytes(), value.as_bytes());
        }
    }

    #[test]
    fn marshalling_rejects_nul_with_operation_context() {
        for kind in ["label", "subscription label", "request label"] {
            let error = RequestLabel::new(Some("bad\0label"), kind).err().unwrap();
            assert!(
                matches!(error, BlpError::InvalidArgument { detail } if detail.starts_with(&format!("invalid {kind}:")))
            );
        }
        assert!(
            matches!(service_name("bad\0service"), Err(BlpError::InvalidArgument { detail }) if detail.starts_with("invalid service name:"))
        );
    }

    #[test]
    fn result_codes_preserve_error_context() {
        assert!(check_result(0, "operation").is_ok());
        assert!(
            matches!(check_result(7, "operation"), Err(BlpError::Internal { detail }) if detail == "operation failed with rc=7")
        );
        assert!(check_service_result(0, "//blp/refdata").is_ok());
        assert!(
            matches!(check_service_result(7, "//blp/refdata"), Err(BlpError::OpenService { service, source: None, label: None }) if service == "//blp/refdata")
        );
    }
}
