//! Schema element definition wrapper.

use std::ffi::CStr;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::rc::Rc;

use crate::ffi;
use crate::name::Name;

use super::SchemaStatus;
use super::type_def::SchemaTypeDefinition;

/// Definition of a schema element (field).
///
/// This is a borrowed view into session-managed schema data. It is not `Send`
/// or `Sync`; copy out owned metadata before crossing threads.
#[derive(Clone, Copy)]
pub struct SchemaElementDefinition<'owner> {
    ptr: *mut ffi::blpapi_SchemaElementDefinition_t,
    _owner: PhantomData<&'owner ()>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl<'owner> SchemaElementDefinition<'owner> {
    /// Create from raw pointer without null check.
    ///
    /// # Safety
    /// The pointer must be non-null and valid for `'owner`.
    pub(crate) unsafe fn from_raw_unchecked(
        ptr: *mut ffi::blpapi_SchemaElementDefinition_t,
    ) -> Self {
        debug_assert!(!ptr.is_null());
        Self {
            ptr,
            _owner: PhantomData,
            _not_send_sync: PhantomData,
        }
    }

    /// Create from raw pointer with null check.
    pub(crate) fn from_raw(ptr: *mut ffi::blpapi_SchemaElementDefinition_t) -> Option<Self> {
        if ptr.is_null() {
            None
        } else {
            Some(Self {
                ptr,
                _owner: PhantomData,
                _not_send_sync: PhantomData,
            })
        }
    }

    /// Get the element name.
    pub fn name(&self) -> Option<Name> {
        unsafe {
            let name_ptr = ffi::blpapi_SchemaElementDefinition_name(self.ptr);
            NonNull::new(name_ptr).map(|ptr| Name::from_raw(ptr))
        }
    }

    /// Get the element name as a string.
    pub fn name_str(&self) -> &str {
        unsafe {
            let name_ptr = ffi::blpapi_SchemaElementDefinition_name(self.ptr);
            if name_ptr.is_null() {
                return "";
            }
            let str_ptr = ffi::blpapi_Name_string(name_ptr);
            if str_ptr.is_null() {
                return "";
            }
            CStr::from_ptr(str_ptr).to_str().unwrap_or("")
        }
    }

    /// Get a human-readable description of this element.
    pub fn description(&self) -> &str {
        unsafe {
            let desc_ptr = ffi::blpapi_SchemaElementDefinition_description(self.ptr);
            if desc_ptr.is_null() {
                return "";
            }
            CStr::from_ptr(desc_ptr).to_str().unwrap_or("")
        }
    }

    /// Get the type definition for this element's values.
    pub fn type_definition(&self) -> SchemaTypeDefinition<'owner> {
        unsafe {
            let type_ptr = ffi::blpapi_SchemaElementDefinition_type(self.ptr);
            SchemaTypeDefinition::from_raw_unchecked(type_ptr)
        }
    }

    /// Get the minimum number of values for this element.
    pub fn min_values(&self) -> usize {
        unsafe { ffi::blpapi_SchemaElementDefinition_minValues(self.ptr) }
    }

    /// Get the maximum number of values for this element.
    pub fn max_values(&self) -> usize {
        unsafe { ffi::blpapi_SchemaElementDefinition_maxValues(self.ptr) }
    }

    /// Check if this element is optional (min_values == 0).
    pub fn is_optional(&self) -> bool {
        self.min_values() == 0
    }

    /// Check if this element is required (min_values >= 1).
    pub fn is_required(&self) -> bool {
        self.min_values() >= 1
    }

    /// Check if this element is an array (max_values > 1).
    pub fn is_array(&self) -> bool {
        self.max_values() > 1
    }

    /// Check if this element is a single value (min == max == 1).
    pub fn is_single_value(&self) -> bool {
        self.min_values() == 1 && self.max_values() == 1
    }

    /// Get the deprecation status of this element.
    pub fn status(&self) -> SchemaStatus {
        unsafe {
            let status = ffi::blpapi_SchemaElementDefinition_status(self.ptr);
            SchemaStatus::from_raw(status)
        }
    }
}

impl std::fmt::Debug for SchemaElementDefinition<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaElementDefinition")
            .field("name", &self.name_str())
            .field("min_values", &self.min_values())
            .field("max_values", &self.max_values())
            .finish()
    }
}
