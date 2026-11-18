//! Bloomberg service operation wrapper.

use std::ffi::CStr;
use std::marker::PhantomData;
use std::rc::Rc;

use crate::errors::{BlpError, Result};
use crate::ffi;
use crate::sdk;

use super::element_def::SchemaElementDefinition;

/// A service operation that can be invoked via requests.
///
/// Operations define the request/response schema for service calls like
/// `ReferenceDataRequest` and `HistoricalDataRequest`. This is a borrowed view
/// into data owned by the parent service/session, so it is not `Send` or `Sync`.
#[derive(Clone, Copy)]
pub struct Operation<'service> {
    ptr: *mut ffi::blpapi_Operation_t,
    _service: PhantomData<&'service ()>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl<'service> Operation<'service> {
    /// Create an Operation from a raw pointer.
    ///
    /// # Safety
    /// The pointer must be non-null and remain valid for `'service`.
    pub(crate) unsafe fn from_raw(ptr: *mut ffi::blpapi_Operation_t) -> Option<Self> {
        if ptr.is_null() {
            None
        } else {
            Some(Self {
                ptr,
                _service: PhantomData,
                _not_send_sync: PhantomData,
            })
        }
    }

    /// Get the operation name (e.g., "ReferenceDataRequest").
    pub fn name(&self) -> &str {
        unsafe {
            let name_ptr = sdk().blpapi_Operation_name(self.ptr);
            if name_ptr.is_null() {
                return "";
            }
            CStr::from_ptr(name_ptr).to_str().unwrap_or("")
        }
    }

    /// Get a human-readable description of this operation.
    pub fn description(&self) -> &str {
        unsafe {
            let desc_ptr = sdk().blpapi_Operation_description(self.ptr);
            if desc_ptr.is_null() {
                return "";
            }
            CStr::from_ptr(desc_ptr).to_str().unwrap_or("")
        }
    }

    /// Get the schema definition for the request.
    pub fn request_definition(&self) -> Result<SchemaElementDefinition<'service>> {
        let mut def_ptr: *mut ffi::blpapi_SchemaElementDefinition_t = std::ptr::null_mut();
        let rc = unsafe { sdk().blpapi_Operation_requestDefinition(self.ptr, &mut def_ptr) };

        if rc != 0 || def_ptr.is_null() {
            return Err(BlpError::Internal {
                detail: format!("Failed to get request definition, rc={}", rc),
            });
        }

        // SAFETY: We verified the pointer is non-null and tie the returned view
        // to this borrowed operation.
        Ok(unsafe { SchemaElementDefinition::from_raw_unchecked(def_ptr) })
    }

    /// Get the number of response type definitions.
    pub fn num_response_definitions(&self) -> usize {
        let count = unsafe { sdk().blpapi_Operation_numResponseDefinitions(self.ptr) };
        count.max(0) as usize
    }

    /// Get a response definition by index.
    pub fn response_definition(&self, index: usize) -> Result<SchemaElementDefinition<'service>> {
        let mut def_ptr: *mut ffi::blpapi_SchemaElementDefinition_t = std::ptr::null_mut();
        let rc =
            unsafe { sdk().blpapi_Operation_responseDefinition(self.ptr, &mut def_ptr, index) };

        if rc != 0 || def_ptr.is_null() {
            return Err(BlpError::Internal {
                detail: format!(
                    "Failed to get response definition at index {}, rc={}",
                    index, rc
                ),
            });
        }

        // SAFETY: We verified the pointer is non-null and tie the returned view
        // to this borrowed operation.
        Ok(unsafe { SchemaElementDefinition::from_raw_unchecked(def_ptr) })
    }

    /// Check if this operation handle is valid.
    pub fn is_valid(&self) -> bool {
        !self.ptr.is_null()
    }
}

impl std::fmt::Debug for Operation<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Operation")
            .field("name", &self.name())
            .field("num_response_definitions", &self.num_response_definitions())
            .finish()
    }
}
