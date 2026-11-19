//! Schema enumeration constant types.

use std::ffi::CStr;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::rc::Rc;

use crate::ffi;
use crate::name::Name;
use crate::sdk;

/// A list of enumeration constants.
///
/// This is a borrowed view into session-managed schema data. It is not `Send`
/// or `Sync`; copy out owned metadata before crossing threads.
#[derive(Clone, Copy)]
pub struct ConstantList<'owner> {
    ptr: *mut ffi::blpapi_ConstantList_t,
    _owner: PhantomData<&'owner ()>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl<'owner> ConstantList<'owner> {
    /// Create from raw pointer with null check.
    pub(crate) fn from_raw(ptr: *mut ffi::blpapi_ConstantList_t) -> Option<Self> {
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

    /// Get the number of constants in this list.
    pub fn len(&self) -> usize {
        let count = unsafe { sdk().blpapi_ConstantList_numConstants(self.ptr) };
        count.max(0) as usize
    }

    /// Check if the list is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get a constant by index.
    pub fn get(&self, index: usize) -> Option<Constant<'owner>> {
        if index >= self.len() {
            return None;
        }

        unsafe {
            let const_ptr = sdk().blpapi_ConstantList_getConstantAt(self.ptr, index);
            Constant::from_raw(const_ptr)
        }
    }

    /// Iterate over all constants in this list.
    pub fn iter(&self) -> ConstantIter<'owner> {
        ConstantIter {
            list: *self,
            index: 0,
            count: self.len(),
        }
    }
}

impl std::fmt::Debug for ConstantList<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConstantList")
            .field("len", &self.len())
            .finish()
    }
}

impl<'owner> IntoIterator for &ConstantList<'owner> {
    type Item = Constant<'owner>;
    type IntoIter = ConstantIter<'owner>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A single enumeration constant value.
///
/// This is a borrowed view into session-managed schema data. It is not `Send`
/// or `Sync`; copy out owned metadata before crossing threads.
#[derive(Clone, Copy)]
pub struct Constant<'owner> {
    ptr: *mut ffi::blpapi_Constant_t,
    _owner: PhantomData<&'owner ()>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl<'owner> Constant<'owner> {
    /// Create from raw pointer with null check.
    pub(crate) fn from_raw(ptr: *mut ffi::blpapi_Constant_t) -> Option<Self> {
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

    /// Get the constant's symbolic name.
    pub fn name(&self) -> Option<Name> {
        unsafe {
            let name_ptr = sdk().blpapi_Constant_name(self.ptr);
            NonNull::new(name_ptr).map(|ptr| Name::from_raw(ptr))
        }
    }

    /// Get the constant's name as a string.
    pub fn name_str(&self) -> &str {
        unsafe {
            let name_ptr = sdk().blpapi_Constant_name(self.ptr);
            if name_ptr.is_null() {
                return "";
            }
            let str_ptr = sdk().blpapi_Name_string(name_ptr);
            if str_ptr.is_null() {
                return "";
            }
            CStr::from_ptr(str_ptr).to_str().unwrap_or("")
        }
    }

    /// Get a human-readable description of this constant.
    pub fn description(&self) -> &str {
        unsafe {
            let desc_ptr = sdk().blpapi_Constant_description(self.ptr);
            if desc_ptr.is_null() {
                return "";
            }
            CStr::from_ptr(desc_ptr).to_str().unwrap_or("")
        }
    }
}

impl std::fmt::Debug for Constant<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Constant")
            .field("name", &self.name_str())
            .finish()
    }
}

impl std::fmt::Display for Constant<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name_str())
    }
}

/// Iterator over constants in a ConstantList.
pub struct ConstantIter<'owner> {
    list: ConstantList<'owner>,
    index: usize,
    count: usize,
}

impl<'owner> Iterator for ConstantIter<'owner> {
    type Item = Constant<'owner>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.count {
            return None;
        }

        let constant = self.list.get(self.index);
        self.index += 1;
        constant
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.count - self.index;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for ConstantIter<'_> {}
