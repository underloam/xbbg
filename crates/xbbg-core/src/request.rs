//! Bloomberg request building

use std::ffi::CString;

use crate::element::Element;
use crate::errors::{BlpError, Result};
use crate::name::Name;

/// Request object for sending to Bloomberg services.
///
/// Requests are created by calling `Service::create_request()` and then
/// populated with data before being sent via `Session::send_request()`. The
/// Bloomberg SDK models requests as mutable single-use handles; this wrapper is
/// intentionally neither `Send` nor `Sync`.
///
/// # Examples
///
/// ```ignore
/// // Pre-intern names at setup (do once)
/// let securities = Name::get_or_intern("securities");
/// let fields = Name::get_or_intern("fields");
///
/// let mut req = svc.create_request("ReferenceDataRequest")?;
///
/// // Add securities
/// req.append_string(&securities, "IBM US Equity")?;
/// req.append_string(&securities, "AAPL US Equity")?;
///
/// // Add fields
/// req.append_string(&fields, "PX_LAST")?;
/// req.append_string(&fields, "SECURITY_NAME")?;
///
/// // Send request
/// session.send_request(&req, None, None)?;
/// ```
pub struct Request {
    ptr: *mut crate::ffi::blpapi_Request_t,
}

impl Request {
    /// Create a Request from a raw pointer (internal use only)
    pub(crate) fn from_raw(ptr: *mut crate::ffi::blpapi_Request_t) -> Result<Self> {
        if ptr.is_null() {
            return Err(BlpError::Internal {
                detail: "null request pointer".into(),
            });
        }
        Ok(Self { ptr })
    }

    /// Get the raw pointer (internal use only)
    pub(crate) fn as_ptr(&self) -> *mut crate::ffi::blpapi_Request_t {
        self.ptr
    }

    /// Get the root element of this request for manipulation.
    ///
    /// This provides low-level access to the request structure.
    /// For most use cases, the convenience methods like `append_string()` are preferred.
    pub fn elements(&self) -> Element<'_> {
        // SAFETY: We're calling the Bloomberg API with a valid pointer
        // The returned element is valid for the lifetime of the Request
        unsafe {
            let elem_ptr = crate::ffi::blpapi_Request_elements(self.ptr);
            Element::new(elem_ptr)
        }
    }

    /// Get a child element by name
    ///
    /// This is a convenience method that calls `elements().get(name)`.
    pub fn get(&self, name: &Name) -> Option<Element<'_>> {
        self.elements().get(name)
    }

    /// Append a string value to an array element.
    ///
    /// This is commonly used for adding securities or fields to a request.
    /// The array element must exist in the request schema.
    ///
    /// # Example
    /// ```ignore
    /// let securities = Name::get_or_intern("securities");
    /// let fields = Name::get_or_intern("fields");
    /// req.append_string(&securities, "IBM US Equity")?;
    /// req.append_string(&securities, "AAPL US Equity")?;
    /// req.append_string(&fields, "PX_LAST")?;
    /// ```
    pub fn append_string(&mut self, array_name: &Name, value: &str) -> Result<()> {
        let root = self.elements();
        let array_elem = root
            .get(array_name)
            .ok_or_else(|| BlpError::InvalidArgument {
                detail: format!("element '{}' not found", array_name.as_str()),
            })?;

        let c_value = CString::new(value).map_err(|e| BlpError::InvalidArgument {
            detail: format!("invalid string value: {}", e),
        })?;

        // SAFETY: We're calling the Bloomberg API with valid pointers
        // - array_elem.as_ptr() is valid for the lifetime of the element
        // - c_value is a valid C string
        // - BLPAPI_ELEMENT_INDEX_END indicates append operation
        let rc = unsafe {
            crate::ffi::blpapi_Element_setValueString(
                array_elem.as_ptr(),
                c_value.as_ptr(),
                crate::ffi::BLPAPI_ELEMENT_INDEX_END as usize,
            )
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("blpapi_Element_setValueString failed with rc={}", rc),
            });
        }

        Ok(())
    }

    /// Append a string value to an array element by string name.
    ///
    /// Convenience method that takes a string name instead of a Name reference.
    /// Slightly slower than `append_string()` but more convenient for simple use cases.
    ///
    /// # Example
    /// ```ignore
    /// req.append_str("securities", "IBM US Equity")?;
    /// req.append_str("securities", "AAPL US Equity")?;
    /// req.append_str("fields", "PX_LAST")?;
    /// ```
    pub fn append_str(&mut self, array_name: &str, value: &str) -> Result<()> {
        let root = self.elements();
        let array_elem = root
            .get_by_str(array_name)
            .ok_or_else(|| BlpError::InvalidArgument {
                detail: format!("element '{}' not found", array_name),
            })?;

        let c_value = CString::new(value).map_err(|e| BlpError::InvalidArgument {
            detail: format!("invalid string value: {}", e),
        })?;

        // SAFETY: array_elem.as_ptr() is valid, c_value is a valid C string.
        // BLPAPI_ELEMENT_INDEX_END indicates append operation.
        let rc = unsafe {
            crate::ffi::blpapi_Element_setValueString(
                array_elem.as_ptr(),
                c_value.as_ptr(),
                crate::ffi::BLPAPI_ELEMENT_INDEX_END as usize,
            )
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("blpapi_Element_setValueString failed with rc={}", rc),
            });
        }

        Ok(())
    }

    /// Set a string value on a scalar element by string name.
    ///
    /// Use this for scalar elements like "startDate", "endDate", "currency".
    /// For array elements like "securities", "fields", use `append_str()` instead.
    ///
    /// # Example
    /// ```ignore
    /// req.set_str("startDate", "20240115")?;
    /// req.set_str("endDate", "20240120")?;
    /// req.set_str("currency", "USD")?;
    /// ```
    pub fn set_str(&mut self, name: &str, value: &str) -> Result<()> {
        let root = self.elements();

        let c_name = CString::new(name).map_err(|e| BlpError::InvalidArgument {
            detail: format!("invalid name: {}", e),
        })?;

        let c_value = CString::new(value).map_err(|e| BlpError::InvalidArgument {
            detail: format!("invalid string value: {}", e),
        })?;

        // SAFETY: We're calling the Bloomberg API with valid pointers
        let rc = unsafe {
            crate::ffi::blpapi_Element_setElementString(
                root.as_ptr(),
                c_name.as_ptr(),
                std::ptr::null(),
                c_value.as_ptr(),
            )
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("set_str('{}') failed with rc={}", name, rc),
            });
        }

        Ok(())
    }

    /// Set a bool value on an element.
    ///
    /// Uses `blpapi_Element_setElementString` with `"true"` / `"false"` because
    /// Bloomberg Bool-typed elements don't accept Int32 values via
    /// `blpapi_Element_setElementInt32` (fails with rc=262156 type mismatch).
    pub fn set_bool(&mut self, name: &Name, value: bool) -> Result<()> {
        let root = self.elements();

        let c_value: &std::ffi::CStr = if value { c"true" } else { c"false" };

        // SAFETY: root and name are valid handles; nameString is null because
        // the interned Name pointer identifies the element.
        let rc = unsafe {
            crate::ffi::blpapi_Element_setElementString(
                root.as_ptr(),
                std::ptr::null(),
                name.as_ptr(),
                c_value.as_ptr(),
            )
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!(
                    "set_bool('{}', {}) failed with rc={}",
                    name.as_str(),
                    value,
                    rc
                ),
            });
        }

        Ok(())
    }

    /// Set a datetime value on a scalar element by string name.
    ///
    /// Parses an ISO-8601 datetime string (e.g., "2024-01-15T09:30:00") and sets
    /// it as a Bloomberg Datetime. Supports formats:
    /// - "2024-01-15T09:30:00" (full datetime)
    /// - "2024-01-15T09:30" (no seconds)
    /// - "2024-01-15" (date only)
    ///
    /// # Example
    /// ```ignore
    /// req.set_datetime("startDateTime", "2024-01-15T09:30:00")?;
    /// req.set_datetime("endDateTime", "2024-01-15T16:00:00")?;
    /// ```
    pub fn set_datetime(&mut self, name: &str, value: &str) -> Result<()> {
        let dt = parse_datetime(value)?;
        let root = self.elements();

        let c_name = CString::new(name).map_err(|e| BlpError::InvalidArgument {
            detail: format!("invalid name: {}", e),
        })?;

        // SAFETY: We're calling the Bloomberg API with valid pointers
        let rc = unsafe {
            crate::ffi::blpapi_Element_setElementDatetime(
                root.as_ptr(),
                c_name.as_ptr(),
                std::ptr::null(),
                &dt,
            )
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("set_datetime('{}') failed with rc={}", name, rc),
            });
        }

        Ok(())
    }

    /// Set an integer value on a scalar element by string name.
    ///
    /// Use this for integer elements like "interval".
    ///
    /// # Example
    /// ```ignore
    /// req.set_int("interval", 5)?;  // 5-minute bars
    /// ```
    pub fn set_int(&mut self, name: &str, value: i32) -> Result<()> {
        let root = self.elements();

        let c_name = CString::new(name).map_err(|e| BlpError::InvalidArgument {
            detail: format!("invalid name: {}", e),
        })?;

        // SAFETY: We're calling the Bloomberg API with valid pointers
        let rc = unsafe {
            crate::ffi::blpapi_Element_setElementInt32(
                root.as_ptr(),
                c_name.as_ptr(),
                std::ptr::null(),
                value,
            )
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("set_int('{}') failed with rc={}", name, rc),
            });
        }

        Ok(())
    }

    /// Append a new element to a sequence/array element and return a mutable handle.
    ///
    /// This is used for Bloomberg override arrays where each entry is a sub-element
    /// with "fieldId" and "value" children.
    ///
    /// # Safety
    ///
    /// `array_element` must be a valid, non-null pointer to a Bloomberg element
    /// obtained from [`get_or_create_element`] or a prior call to this method.
    ///
    /// # Example
    /// ```ignore
    /// // Get the overrides array
    /// let overrides_ptr = req.get_or_create_element("overrides")?;
    /// // Append a new override entry
    /// let entry_ptr = unsafe { req.append_element(overrides_ptr)? };
    /// // Set fieldId and value on the entry
    /// unsafe { req.set_element_string(entry_ptr, "fieldId", "BEST_FPERIOD_OVERRIDE")? };
    /// unsafe { req.set_element_string(entry_ptr, "value", "1FY")? };
    /// ```
    pub unsafe fn append_element(
        &mut self,
        array_element: *mut crate::ffi::blpapi_Element_t,
    ) -> Result<*mut crate::ffi::blpapi_Element_t> {
        let mut appended = std::mem::MaybeUninit::uninit();

        let rc = unsafe {
            crate::ffi::blpapi_Element_appendElement(array_element, appended.as_mut_ptr())
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("blpapi_Element_appendElement failed with rc={}", rc),
            });
        }

        Ok(unsafe { appended.assume_init() })
    }

    /// Set a string value on an element pointer by name.
    ///
    /// This is the low-level version that operates on a raw element pointer
    /// rather than the request root. Used for setting fields on sub-elements
    /// returned by `append_element()`.
    ///
    /// # Safety
    ///
    /// `element` must be a valid, non-null pointer to a Bloomberg element
    /// obtained from [`get_or_create_element`] or [`append_element`].
    pub unsafe fn set_element_string(
        &mut self,
        element: *mut crate::ffi::blpapi_Element_t,
        name: &str,
        value: &str,
    ) -> Result<()> {
        let c_name = CString::new(name).map_err(|e| BlpError::InvalidArgument {
            detail: format!("invalid name: {}", e),
        })?;
        let c_value = CString::new(value).map_err(|e| BlpError::InvalidArgument {
            detail: format!("invalid value: {}", e),
        })?;

        // SAFETY: element is a valid pointer from get_or_create_element/append_element
        let rc = unsafe {
            crate::ffi::blpapi_Element_setElementString(
                element,
                c_name.as_ptr(),
                std::ptr::null(),
                c_value.as_ptr(),
            )
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("set_element_string('{}') failed with rc={}", name, rc),
            });
        }

        Ok(())
    }

    /// Get or create a child element by name.
    ///
    /// For sequence/choice elements, this will create the sub-element if it doesn't exist.
    /// Returns the element pointer for further manipulation.
    ///
    /// # Example
    /// ```ignore
    /// let price_source = req.get_or_create_element("priceSource")?;
    /// ```
    pub fn get_or_create_element(
        &mut self,
        name: &str,
    ) -> Result<*mut crate::ffi::blpapi_Element_t> {
        let root = self.elements();
        let mut out = std::mem::MaybeUninit::uninit();

        let c_name = CString::new(name).map_err(|e| BlpError::InvalidArgument {
            detail: format!("invalid name: {}", e),
        })?;

        // blpapi_Element_getElement creates the sub-element for sequences/choices if it doesn't exist
        let rc = unsafe {
            crate::ffi::blpapi_Element_getElement(
                root.as_ptr(),
                out.as_mut_ptr(),
                c_name.as_ptr(),
                std::ptr::null(),
            )
        };

        if rc != 0 {
            return Err(BlpError::InvalidArgument {
                detail: format!("get_or_create_element('{}') failed with rc={}", name, rc),
            });
        }

        Ok(unsafe { out.assume_init() })
    }

    /// Set a string value on a nested element using a dotted path.
    ///
    /// Navigates through the element tree using the path segments, creating
    /// intermediate elements as needed, then sets the value on the leaf element.
    ///
    /// # Example
    /// ```ignore
    /// // Sets priceSource -> securityName = "AAPL US Equity"
    /// req.set_nested_str("priceSource.securityName", "AAPL US Equity")?;
    ///
    /// // Sets priceSource -> dataRange -> historical -> startDate = "20240101"
    /// req.set_nested_str("priceSource.dataRange.historical.startDate", "20240101")?;
    /// ```
    pub fn set_nested_str(&mut self, path: &str, value: &str) -> Result<()> {
        let segments: Vec<&str> = path.split('.').collect();
        if segments.is_empty() {
            return Err(BlpError::InvalidArgument {
                detail: "empty path".into(),
            });
        }

        // Navigate to parent element (all but last segment)
        let mut current = self.elements().as_ptr();
        for segment in &segments[..segments.len() - 1] {
            let c_name = CString::new(*segment).map_err(|e| BlpError::InvalidArgument {
                detail: format!("invalid segment '{}': {}", segment, e),
            })?;

            let mut next = std::mem::MaybeUninit::uninit();
            let rc = unsafe {
                crate::ffi::blpapi_Element_getElement(
                    current,
                    next.as_mut_ptr(),
                    c_name.as_ptr(),
                    std::ptr::null(),
                )
            };

            if rc != 0 {
                return Err(BlpError::InvalidArgument {
                    detail: format!("failed to navigate to '{}' in path '{}'", segment, path),
                });
            }

            current = unsafe { next.assume_init() };
        }

        // Set value on the leaf element
        let leaf_name = segments.last().unwrap();
        let c_name = CString::new(*leaf_name).map_err(|e| BlpError::InvalidArgument {
            detail: format!("invalid name '{}': {}", leaf_name, e),
        })?;
        let c_value = CString::new(value).map_err(|e| BlpError::InvalidArgument {
            detail: format!("invalid value: {}", e),
        })?;

        let rc = unsafe {
            crate::ffi::blpapi_Element_setElementString(
                current,
                c_name.as_ptr(),
                std::ptr::null(),
                c_value.as_ptr(),
            )
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("set_nested_str('{}') failed with rc={}", path, rc),
            });
        }

        Ok(())
    }

    /// Set an integer value on a nested element using a dotted path.
    ///
    /// # Example
    /// ```ignore
    /// req.set_nested_int("studyAttributes.smavgStudyAttributes.period", 20)?;
    /// ```
    pub fn set_nested_int(&mut self, path: &str, value: i32) -> Result<()> {
        let segments: Vec<&str> = path.split('.').collect();
        if segments.is_empty() {
            return Err(BlpError::InvalidArgument {
                detail: "empty path".into(),
            });
        }

        // Navigate to parent element (all but last segment)
        let mut current = self.elements().as_ptr();
        for segment in &segments[..segments.len() - 1] {
            let c_name = CString::new(*segment).map_err(|e| BlpError::InvalidArgument {
                detail: format!("invalid segment '{}': {}", segment, e),
            })?;

            let mut next = std::mem::MaybeUninit::uninit();
            let rc = unsafe {
                crate::ffi::blpapi_Element_getElement(
                    current,
                    next.as_mut_ptr(),
                    c_name.as_ptr(),
                    std::ptr::null(),
                )
            };

            if rc != 0 {
                return Err(BlpError::InvalidArgument {
                    detail: format!("failed to navigate to '{}' in path '{}'", segment, path),
                });
            }

            current = unsafe { next.assume_init() };
        }

        // Set value on the leaf element
        let leaf_name = segments.last().unwrap();
        let c_name = CString::new(*leaf_name).map_err(|e| BlpError::InvalidArgument {
            detail: format!("invalid name '{}': {}", leaf_name, e),
        })?;

        let rc = unsafe {
            crate::ffi::blpapi_Element_setElementInt32(
                current,
                c_name.as_ptr(),
                std::ptr::null(),
                value,
            )
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("set_nested_int('{}') failed with rc={}", path, rc),
            });
        }

        Ok(())
    }
}

/// Parse an ISO-8601 datetime string into a Bloomberg Datetime struct.
///
/// Supports formats:
/// - "2024-01-15T09:30:00" (full datetime)
/// - "2024-01-15T09:30" (no seconds)
/// - "2024-01-15" (date only)
fn split_datetime_offset(time_str: &str) -> Result<(&str, Option<i16>)> {
    if let Some(stripped) = time_str
        .strip_suffix('Z')
        .or_else(|| time_str.strip_suffix('z'))
    {
        return Ok((stripped, Some(0)));
    }

    if let Some((idx, sign)) = time_str
        .char_indices()
        .rev()
        .find(|(idx, ch)| *idx > 0 && (*ch == '+' || *ch == '-'))
    {
        let offset = parse_datetime_offset(sign, &time_str[idx + 1..])?;
        return Ok((&time_str[..idx], Some(offset)));
    }

    Ok((time_str, None))
}

fn parse_datetime_offset(sign: char, offset: &str) -> Result<i16> {
    let (hours_raw, minutes_raw) = if let Some((hours, minutes)) = offset.split_once(':') {
        (hours, minutes)
    } else if offset.len() == 4 {
        offset.split_at(2)
    } else {
        (offset, "0")
    };

    let hours: i16 = hours_raw.parse().map_err(|_| BlpError::InvalidArgument {
        detail: format!("invalid timezone offset hours: {}", hours_raw),
    })?;
    let minutes: i16 = minutes_raw.parse().map_err(|_| BlpError::InvalidArgument {
        detail: format!("invalid timezone offset minutes: {}", minutes_raw),
    })?;

    if hours > 14 || (hours == 14 && minutes > 0) || minutes > 59 {
        return Err(BlpError::InvalidArgument {
            detail: format!("invalid timezone offset: {}{}", sign, offset),
        });
    }

    let offset_minutes = hours * 60 + minutes;
    Ok(if sign == '-' {
        -offset_minutes
    } else {
        offset_minutes
    })
}

fn parse_datetime(s: &str) -> Result<crate::ffi::blpapi_Datetime_t> {
    use crate::ffi::{
        BLPAPI_DATETIME_DATE_PART, BLPAPI_DATETIME_HOURS_PART, BLPAPI_DATETIME_MINUTES_PART,
        BLPAPI_DATETIME_OFFSET_PART, BLPAPI_DATETIME_SECONDS_PART, blpapi_Datetime_t,
    };

    let mut dt = blpapi_Datetime_t::default();

    // Split date and time parts
    let (date_part, time_part) = if let Some(idx) = s.find('T') {
        (&s[..idx], Some(&s[idx + 1..]))
    } else {
        (s, None)
    };

    // Parse date: "2024-01-15"
    let date_parts: Vec<&str> = date_part.split('-').collect();
    if date_parts.len() != 3 {
        return Err(BlpError::InvalidArgument {
            detail: format!("invalid date format: {}", date_part),
        });
    }

    dt.year = date_parts[0]
        .parse()
        .map_err(|_| BlpError::InvalidArgument {
            detail: format!("invalid year: {}", date_parts[0]),
        })?;
    dt.month = date_parts[1]
        .parse()
        .map_err(|_| BlpError::InvalidArgument {
            detail: format!("invalid month: {}", date_parts[1]),
        })?;
    dt.day = date_parts[2]
        .parse()
        .map_err(|_| BlpError::InvalidArgument {
            detail: format!("invalid day: {}", date_parts[2]),
        })?;
    dt.parts = BLPAPI_DATETIME_DATE_PART;

    // Parse time if present: "09:30:00", "09:30", or with an ISO timezone suffix.
    if let Some(raw_time_str) = time_part {
        let (time_str, offset_minutes) = split_datetime_offset(raw_time_str)?;
        if time_str.is_empty() {
            return Err(BlpError::InvalidArgument {
                detail: format!("invalid datetime time: {}", s),
            });
        }

        let time_parts: Vec<&str> = time_str.split(':').collect();
        if !(2..=3).contains(&time_parts.len()) || time_parts.iter().any(|part| part.is_empty()) {
            return Err(BlpError::InvalidArgument {
                detail: format!("invalid datetime time: {}", time_str),
            });
        }

        dt.hours = time_parts[0]
            .parse()
            .map_err(|_| BlpError::InvalidArgument {
                detail: format!("invalid hours: {}", time_parts[0]),
            })?;
        dt.minutes = time_parts[1]
            .parse()
            .map_err(|_| BlpError::InvalidArgument {
                detail: format!("invalid minutes: {}", time_parts[1]),
            })?;
        if dt.hours > 23 || dt.minutes > 59 {
            return Err(BlpError::InvalidArgument {
                detail: format!("invalid datetime time: {}", time_str),
            });
        }
        dt.parts |= BLPAPI_DATETIME_HOURS_PART | BLPAPI_DATETIME_MINUTES_PART;

        if time_parts.len() == 3 {
            // Handle seconds, possibly with fractional part.
            let sec_str = time_parts[2].split('.').next().unwrap_or("0");
            if sec_str.is_empty() {
                return Err(BlpError::InvalidArgument {
                    detail: format!("invalid seconds: {}", time_parts[2]),
                });
            }
            dt.seconds = sec_str.parse().map_err(|_| BlpError::InvalidArgument {
                detail: format!("invalid seconds: {}", sec_str),
            })?;
            if dt.seconds > 59 {
                return Err(BlpError::InvalidArgument {
                    detail: format!("invalid seconds: {}", sec_str),
                });
            }
            dt.parts |= BLPAPI_DATETIME_SECONDS_PART;
        }

        if let Some(offset) = offset_minutes {
            dt.offset = offset;
            dt.parts |= BLPAPI_DATETIME_OFFSET_PART;
        }
    }

    Ok(dt)
}

impl Drop for Request {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: We own this pointer and it's valid.
            // blpapi_Request_destroy releases the request resources.
            unsafe {
                crate::ffi::blpapi_Request_destroy(self.ptr);
            }
            self.ptr = std::ptr::null_mut();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_datetime_accepts_zero_offset_suffix() {
        let dt = parse_datetime("2026-04-24T14:30:00+00:00").unwrap();

        assert_eq!(dt.hours, 14);
        assert_eq!(dt.minutes, 30);
        assert_eq!(dt.seconds, 0);
        assert_eq!(dt.offset, 0);
        assert!(dt.parts & crate::ffi::BLPAPI_DATETIME_OFFSET_PART != 0);
    }

    #[test]
    fn parse_datetime_accepts_nonzero_offset_suffix() {
        let dt = parse_datetime("2026-04-24T14:30:03+02:00").unwrap();

        assert_eq!(dt.seconds, 3);
        assert_eq!(dt.offset, 120);
        assert!(dt.parts & crate::ffi::BLPAPI_DATETIME_OFFSET_PART != 0);
    }

    #[test]
    fn parse_datetime_accepts_z_suffix() {
        let dt = parse_datetime("2026-04-24T14:30:03Z").unwrap();

        assert_eq!(dt.seconds, 3);
        assert_eq!(dt.offset, 0);
        assert!(dt.parts & crate::ffi::BLPAPI_DATETIME_OFFSET_PART != 0);
    }

    #[test]
    fn parse_datetime_rejects_empty_time_after_t() {
        assert!(parse_datetime("2026-04-24T").is_err());
    }

    #[test]
    fn parse_datetime_rejects_single_component_time_after_t() {
        assert!(parse_datetime("2026-04-24T14").is_err());
    }

    #[test]
    fn parse_datetime_rejects_out_of_range_time() {
        assert!(parse_datetime("2026-04-24T24:00:00").is_err());
        assert!(parse_datetime("2026-04-24T14:60:00").is_err());
        assert!(parse_datetime("2026-04-24T14:30:60").is_err());
    }

    #[test]
    fn parse_datetime_rejects_sdk_invalid_offset_range() {
        assert!(parse_datetime("2026-04-24T14:30:00+14:01").is_err());
        assert!(parse_datetime("2026-04-24T14:30:00-14:01").is_err());
    }
}
