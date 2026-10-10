//! Schema type definition wrapper.

use std::ffi::CStr;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::rc::Rc;

use crate::datatype::DataType;
use crate::ffi;
use crate::name::Name;

use super::constant::ConstantList;
use super::element_def::SchemaElementDefinition;
use super::SchemaStatus;

/// Definition of a schema type.
///
/// This is a borrowed view into session-managed schema data. It is not `Send`
/// or `Sync`; copy out owned metadata before crossing threads.
#[derive(Clone, Copy)]
pub struct SchemaTypeDefinition<'owner> {
    ptr: *mut ffi::blpapi_SchemaTypeDefinition_t,
    _owner: PhantomData<&'owner ()>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl<'owner> SchemaTypeDefinition<'owner> {
    /// Create from raw pointer without null check.
    ///
    /// # Safety
    /// The pointer must be non-null and valid for `'owner`.
    pub(crate) unsafe fn from_raw_unchecked(ptr: *mut ffi::blpapi_SchemaTypeDefinition_t) -> Self {
        debug_assert!(!ptr.is_null());
        Self {
            ptr,
            _owner: PhantomData,
            _not_send_sync: PhantomData,
        }
    }

    /// Get the type name.
    pub fn name(&self) -> Option<Name> {
        unsafe {
            let name_ptr = ffi::blpapi_SchemaTypeDefinition_name(self.ptr);
            NonNull::new(name_ptr).map(|ptr| Name::from_raw(ptr))
        }
    }

    /// Get the type name as a string.
    pub fn name_str(&self) -> &str {
        unsafe {
            let name_ptr = ffi::blpapi_SchemaTypeDefinition_name(self.ptr);
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

    /// Get a human-readable description of this type.
    pub fn description(&self) -> &str {
        unsafe {
            let desc_ptr = ffi::blpapi_SchemaTypeDefinition_description(self.ptr);
            if desc_ptr.is_null() {
                return "";
            }
            CStr::from_ptr(desc_ptr).to_str().unwrap_or("")
        }
    }

    /// Get the underlying data type.
    pub fn datatype(&self) -> DataType {
        unsafe {
            let dt = ffi::blpapi_SchemaTypeDefinition_datatype(self.ptr);
            DataType::from_raw(dt)
        }
    }

    /// Check if this is a complex type (sequence or choice).
    pub fn is_complex_type(&self) -> bool {
        unsafe { ffi::blpapi_SchemaTypeDefinition_isComplexType(self.ptr) != 0 }
    }

    /// Check if this is a simple type.
    pub fn is_simple_type(&self) -> bool {
        unsafe { ffi::blpapi_SchemaTypeDefinition_isSimpleType(self.ptr) != 0 }
    }

    /// Check if this is an enumeration type.
    pub fn is_enumeration_type(&self) -> bool {
        unsafe { ffi::blpapi_SchemaTypeDefinition_isEnumerationType(self.ptr) != 0 }
    }

    /// Get the number of child element definitions.
    pub fn num_element_definitions(&self) -> usize {
        unsafe { ffi::blpapi_SchemaTypeDefinition_numElementDefinitions(self.ptr) }
    }

    /// Get a child element definition by index.
    pub fn get_element_definition(&self, index: usize) -> Option<SchemaElementDefinition<'owner>> {
        if index >= self.num_element_definitions() {
            return None;
        }

        unsafe {
            let elem_ptr = ffi::blpapi_SchemaTypeDefinition_getElementDefinitionAt(self.ptr, index);
            SchemaElementDefinition::from_raw(elem_ptr)
        }
    }

    /// Get the enumeration values for this type.
    pub fn enumeration(&self) -> Option<ConstantList<'owner>> {
        if !self.is_enumeration_type() {
            return None;
        }

        unsafe {
            let list_ptr = ffi::blpapi_SchemaTypeDefinition_enumeration(self.ptr);
            ConstantList::from_raw(list_ptr)
        }
    }

    /// Iterate over all child element definitions.
    pub fn element_definitions(&self) -> ElementDefinitionIter<'owner> {
        ElementDefinitionIter {
            type_def: *self,
            index: 0,
            count: self.num_element_definitions(),
        }
    }

    /// Get the deprecation status of this type.
    pub fn status(&self) -> SchemaStatus {
        unsafe {
            let status = ffi::blpapi_SchemaTypeDefinition_status(self.ptr);
            SchemaStatus::from_raw(status)
        }
    }
}

impl std::fmt::Debug for SchemaTypeDefinition<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaTypeDefinition")
            .field("name", &self.name_str())
            .field("datatype", &self.datatype())
            .field("is_complex", &self.is_complex_type())
            .field("is_enumeration", &self.is_enumeration_type())
            .field("num_elements", &self.num_element_definitions())
            .finish()
    }
}

/// Iterator over element definitions in a complex type.
pub struct ElementDefinitionIter<'owner> {
    type_def: SchemaTypeDefinition<'owner>,
    index: usize,
    count: usize,
}

impl<'owner> Iterator for ElementDefinitionIter<'owner> {
    type Item = SchemaElementDefinition<'owner>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.count {
            return None;
        }

        let elem = self.type_def.get_element_definition(self.index);
        self.index += 1;
        elem
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.count - self.index;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for ElementDefinitionIter<'_> {}
