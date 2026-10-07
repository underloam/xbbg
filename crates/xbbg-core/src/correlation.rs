//! Correlation ID for tracking requests and subscriptions

use std::ffi::c_void;
use std::marker::PhantomData;
use std::rc::Rc;

/// Opaque pointer correlation value.
///
/// Constructing one is unsafe because the Bloomberg SDK will copy the pointer
/// value through asynchronous request/subscription lifetimes. Callers must
/// ensure the pointer remains valid for the entire Bloomberg operation and that
/// any later dereference is synchronized by the application.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PointerCorrelationId {
    ptr: *mut c_void,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl PointerCorrelationId {
    /// Return the raw pointer value.
    pub fn as_ptr(self) -> *mut c_void {
        self.ptr
    }
}

/// Correlation ID used to match requests/subscriptions with responses.
///
/// Integer correlation IDs are the normal Rust API. Pointer IDs are retained for
/// low-level Bloomberg interop but require `unsafe` construction.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum CorrelationId {
    /// Unset correlation ID. The Bloomberg SDK may autogenerate one internally.
    #[default]
    Unset,
    /// Integer correlation ID.
    Int(i64),
    /// Pointer correlation ID for advanced interop.
    Ptr(PointerCorrelationId),
}

impl CorrelationId {
    /// Create a new integer correlation ID.
    pub fn new_int(value: i64) -> Self {
        CorrelationId::Int(value)
    }

    /// Create a new pointer correlation ID.
    ///
    /// # Safety
    /// The pointer must remain valid until the Bloomberg operation using this
    /// correlation ID is complete, and any application dereference must be
    /// externally synchronized.
    pub unsafe fn new_ptr(ptr: *mut c_void) -> Self {
        CorrelationId::Ptr(PointerCorrelationId {
            ptr,
            _not_send_sync: PhantomData,
        })
    }

    /// Get the pointer value if this is a Ptr variant.
    pub fn as_ptr(&self) -> Option<*mut c_void> {
        match self {
            CorrelationId::Ptr(p) => Some(p.as_ptr()),
            _ => None,
        }
    }

    /// Convert to FFI representation.
    pub(crate) fn to_ffi(&self) -> crate::ffi::blpapi_CorrelationId_t {
        // SAFETY: zeroed memory is valid for blpapi_CorrelationId_t, then we
        // set the appropriate bitfields and union member.
        unsafe {
            let mut cid = std::mem::zeroed::<crate::ffi::blpapi_CorrelationId_t>();
            cid.set_size(std::mem::size_of::<crate::ffi::blpapi_CorrelationId_t>() as u32);
            match self {
                CorrelationId::Unset => {
                    cid.set_valueType(crate::ffi::BLPAPI_CORRELATION_TYPE_UNSET);
                }
                CorrelationId::Int(v) => {
                    cid.set_valueType(crate::ffi::BLPAPI_CORRELATION_TYPE_INT);
                    cid.value.intValue = *v as u64;
                }
                CorrelationId::Ptr(p) => {
                    cid.set_valueType(crate::ffi::BLPAPI_CORRELATION_TYPE_POINTER);
                    cid.value.ptrValue.pointer = p.as_ptr();
                }
            }
            cid
        }
    }

    /// Create from FFI representation.
    pub fn from_ffi(cid: &crate::ffi::blpapi_CorrelationId_t) -> Self {
        let value_type = cid.valueType();
        match value_type {
            x if x == crate::ffi::BLPAPI_CORRELATION_TYPE_UNSET => CorrelationId::Unset,
            x if x == crate::ffi::BLPAPI_CORRELATION_TYPE_INT => {
                // SAFETY: valueType indicates this is an int value.
                let value = unsafe { cid.value.intValue };
                CorrelationId::Int(value as i64)
            }
            x if x == crate::ffi::BLPAPI_CORRELATION_TYPE_AUTOGEN => {
                // SAFETY: Bloomberg documents AUTOGEN correlation IDs as integer-valued.
                let value = unsafe { cid.value.intValue };
                CorrelationId::Int(value as i64)
            }
            x if x == crate::ffi::BLPAPI_CORRELATION_TYPE_POINTER => {
                // SAFETY: valueType indicates this is a pointer value. The SDK
                // has already produced it; application code remains responsible
                // for any later dereference.
                let ptr = unsafe { cid.value.ptrValue.pointer };
                // SAFETY: see comment above. We only preserve the opaque value.
                unsafe { CorrelationId::new_ptr(ptr) }
            }
            _ => CorrelationId::Unset,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_unset() {
        assert_eq!(CorrelationId::default(), CorrelationId::Unset);
    }

    #[test]
    fn unset_round_trips_through_ffi() {
        let cid = CorrelationId::Unset;
        let ffi_cid = cid.to_ffi();

        assert_eq!(
            ffi_cid.valueType(),
            crate::ffi::BLPAPI_CORRELATION_TYPE_UNSET
        );
        assert_eq!(CorrelationId::from_ffi(&ffi_cid), CorrelationId::Unset);
    }

    #[test]
    fn autogen_ffi_maps_to_integer_correlation_id() {
        let mut ffi_cid = unsafe { std::mem::zeroed::<crate::ffi::blpapi_CorrelationId_t>() };
        ffi_cid.set_size(std::mem::size_of::<crate::ffi::blpapi_CorrelationId_t>() as u32);
        ffi_cid.set_valueType(crate::ffi::BLPAPI_CORRELATION_TYPE_AUTOGEN);
        ffi_cid.value.intValue = 123;

        assert_eq!(CorrelationId::from_ffi(&ffi_cid), CorrelationId::Int(123));
    }
}
