//! Bloomberg service handle
//!
//! Bloomberg owns service data through the `Session` that opened it. This
//! wrapper is therefore a borrowed, non-thread-safe view tied to that session.

use std::ffi::{CStr, CString};
use std::marker::PhantomData;
use std::rc::Rc;

use crate::errors::{BlpError, Result};
use crate::request::Request;
use crate::schema::Operation;

/// Service handle for creating requests.
///
/// A `Service` is obtained from a `Session` after opening the service. The
/// Bloomberg SDK owns the underlying service data through that session, so this
/// handle must not outlive the session and is not `Send` or `Sync`.
///
/// # Examples
///
/// ```ignore
/// session.open_service("//blp/refdata")?;
/// let svc = session.get_service("//blp/refdata")?;
/// let req = svc.create_request("ReferenceDataRequest")?;
/// ```
pub struct Service<'session> {
    ptr: *mut crate::ffi::blpapi_Service_t,
    _session: PhantomData<&'session crate::session::Session>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl<'session> Service<'session> {
    /// Create a Service from a raw pointer returned by the owning session.
    pub(crate) fn from_raw(ptr: *mut crate::ffi::blpapi_Service_t) -> Result<Self> {
        if ptr.is_null() {
            return Err(BlpError::Internal {
                detail: "null service pointer".into(),
            });
        }
        Ok(Self {
            ptr,
            _session: PhantomData,
            _not_send_sync: PhantomData,
        })
    }

    /// Get the raw pointer (internal use only).
    pub(crate) fn as_ptr(&self) -> *mut crate::ffi::blpapi_Service_t {
        self.ptr
    }

    /// Create a new request for the specified operation.
    ///
    /// Common operations:
    /// - `"ReferenceDataRequest"` - Get reference data for securities
    /// - `"HistoricalDataRequest"` - Get historical time series data
    /// - `"IntradayBarRequest"` - Get intraday bar data
    pub fn create_request(&self, operation: &str) -> Result<Request> {
        let c_operation = CString::new(operation).map_err(|e| BlpError::InvalidArgument {
            detail: format!("invalid operation name: {}", e),
        })?;

        let mut req_ptr: *mut crate::ffi::blpapi_Request_t = std::ptr::null_mut();

        // SAFETY: self.ptr is a non-null service pointer borrowed from a live
        // Session, req_ptr is an out-parameter, and c_operation is a valid C string.
        let rc = unsafe {
            crate::ffi::blpapi_Service_createRequest(self.ptr, &mut req_ptr, c_operation.as_ptr())
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("blpapi_Service_createRequest failed with rc={}", rc),
            });
        }

        Request::from_raw(req_ptr)
    }

    /// Get the service name.
    pub fn name(&self) -> &str {
        // SAFETY: the SDK returns a service-owned C string valid while this
        // borrowed service handle remains valid.
        unsafe {
            let name_ptr = crate::ffi::blpapi_Service_name(self.ptr);
            if name_ptr.is_null() {
                return "";
            }

            CStr::from_ptr(name_ptr).to_str().unwrap_or("")
        }
    }

    /// Get a human-readable description of this service.
    pub fn description(&self) -> &str {
        // SAFETY: the SDK returns a service-owned C string valid while this
        // borrowed service handle remains valid.
        unsafe {
            let desc_ptr = crate::ffi::blpapi_Service_description(self.ptr);
            if desc_ptr.is_null() {
                return "";
            }
            CStr::from_ptr(desc_ptr).to_str().unwrap_or("")
        }
    }

    /// Get the number of operations defined by this service.
    pub fn num_operations(&self) -> usize {
        let count = unsafe { crate::ffi::blpapi_Service_numOperations(self.ptr) };
        count.max(0) as usize
    }

    /// Get an operation by index.
    pub fn get_operation_at(&self, index: usize) -> Result<Operation<'session>> {
        if index >= self.num_operations() {
            return Err(BlpError::InvalidArgument {
                detail: format!(
                    "Operation index {} out of bounds (service has {} operations)",
                    index,
                    self.num_operations()
                ),
            });
        }

        let mut op_ptr: *mut crate::ffi::blpapi_Operation_t = std::ptr::null_mut();

        let rc = unsafe { crate::ffi::blpapi_Service_getOperationAt(self.ptr, &mut op_ptr, index) };

        if rc != 0 || op_ptr.is_null() {
            return Err(BlpError::Internal {
                detail: format!("Failed to get operation at index {}, rc={}", index, rc),
            });
        }

        // SAFETY: We verified the pointer is non-null and the Operation cannot
        // outlive this borrowed Service reference.
        unsafe { Operation::from_raw(op_ptr) }.ok_or_else(|| BlpError::Internal {
            detail: "Received null operation pointer".into(),
        })
    }

    /// Iterate over all operations in this service.
    pub fn operations(&self) -> OperationIter<'_, 'session> {
        OperationIter {
            service: self,
            index: 0,
            count: self.num_operations(),
        }
    }
}

/// Iterator over operations in a service.
pub struct OperationIter<'service, 'session> {
    service: &'service Service<'session>,
    index: usize,
    count: usize,
}

impl<'service, 'session> Iterator for OperationIter<'service, 'session> {
    type Item = Operation<'session>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.count {
            return None;
        }

        let op = self.service.get_operation_at(self.index).ok();
        self.index += 1;
        op
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.count - self.index;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for OperationIter<'_, '_> {}
