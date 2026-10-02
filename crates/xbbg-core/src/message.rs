//! Message type for Bloomberg BLPAPI
//!
//! Messages are the primary data containers in Bloomberg responses.
//! Each message contains a root element with field data.
//!
//! **Zero allocation**: Messages are borrowed from the event's message
//! iterator and provide zero-cost access to their contents.

use crate::{ffi, CorrelationId, Element, Name};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::rc::Rc;

/// Bloomberg message wrapper.
///
/// Borrowed from the [`MessageIterator`](crate::event::MessageIterator) that
/// yielded it. NOT thread-safe - must be consumed on receiving thread.
///
/// # Lifetime
/// The lifetime `'a` is the borrow of that iterator: Bloomberg keeps a message
/// valid only until its iterator advances or is destroyed. Elements and strings
/// read from the message share `'a`, so extract owned data before advancing.
///
/// # Thread Safety
/// Messages are `!Send + !Sync` because:
/// - Bloomberg's API is not thread-safe
/// - Messages must be processed on the thread that received them
///
/// # Performance
/// All methods are `#[inline(always)]` for zero-cost abstraction.
#[repr(transparent)]
pub struct Message<'a> {
    ptr: *mut ffi::blpapi_Message_t,
    _life: PhantomData<&'a ()>,
    _marker: PhantomData<Rc<()>>, // Makes !Send + !Sync
}

impl<'a> Message<'a> {
    /// Construct from raw pointer (internal use only).
    ///
    /// # Safety
    /// Caller must ensure:
    /// - `ptr` is a valid `blpapi_Message_t` pointer
    /// - The pointer remains valid for the lifetime `'a`; for a message from a
    ///   `MessageIterator`, `'a` must end before the iterator advances or is
    ///   destroyed
    #[inline]
    pub(crate) unsafe fn from_raw(ptr: *mut ffi::blpapi_Message_t) -> Self {
        Self {
            ptr,
            _life: PhantomData,
            _marker: PhantomData,
        }
    }

    /// Get root element of this message.
    ///
    /// The root element contains all field data for this message.
    /// Use this to navigate the message structure.
    ///
    /// # Performance
    /// This is a hot path method - returns immediately with no allocation.
    #[inline(always)]
    pub fn elements(&self) -> Element<'a> {
        // SAFETY: blpapi_Message_elements returns a valid Element pointer.
        // The Element borrows from this Message, so lifetime 'a is correct.
        // Bloomberg guarantees the element pointer is valid for the message's lifetime.
        // Element::new is safe to call with a valid pointer.
        let ptr = unsafe { ffi::blpapi_Message_elements(self.ptr) };
        Element::new(ptr)
    }

    /// Message type name.
    ///
    /// Returns the schema type of this message (e.g., "ReferenceDataResponse").
    /// This is an owned Name because it's duplicated from Bloomberg's internal storage.
    ///
    /// # Performance
    /// This allocates a new Name (increments refcount). Cache if called repeatedly.
    #[inline(always)]
    pub fn message_type(&self) -> Name {
        self.name()
    }

    /// Message name (alias for `message_type()`).
    ///
    /// Returns the schema type of this message (e.g., "ReferenceDataResponse").
    /// This is an owned Name because it's duplicated from Bloomberg's internal storage.
    ///
    /// # Performance
    /// This allocates a new Name (increments refcount). Cache if called repeatedly.
    #[inline(always)]
    pub fn name(&self) -> Name {
        // SAFETY: blpapi_Message_messageType returns a valid Name pointer.
        // We duplicate it to get an owned Name that we can return.
        // The duplicate increments Bloomberg's internal refcount.
        let ptr = unsafe { ffi::blpapi_Message_messageType(self.ptr) };
        // SAFETY: blpapi_Name_duplicate returns a valid pointer
        unsafe { Name::from_raw(NonNull::new(ffi::blpapi_Name_duplicate(ptr)).unwrap()) }
    }

    /// Message type as a borrowed string — no `Name` duplication, no refcount.
    ///
    /// Uses `blpapi_Message_typeString` (blpapi_message.h), which returns a
    /// pointer into the message's own storage. Valid for the message lifetime.
    ///
    /// # Performance
    /// Hot-path alternative to `message_type().as_str()`: avoids the
    /// duplicate/drop refcount pair and the `Name` allocation.
    #[inline(always)]
    pub fn type_str(&self) -> &'a str {
        // SAFETY: blpapi_Message_typeString returns a valid null-terminated
        // C string owned by the message, valid for the message lifetime 'a.
        let ptr = unsafe { ffi::blpapi_Message_typeString(self.ptr) };
        unsafe { std::ffi::CStr::from_ptr(ptr) }
            .to_str()
            .expect("Bloomberg message type contained invalid UTF-8")
    }

    /// Message type intern key for allocation-free dispatch.
    ///
    /// Bloomberg message-type `Name`s are interned; the pointer value is a
    /// stable process-lifetime key for the type. Use as a transient map key
    /// while dispatching, mirroring [`Element::name_key`].
    #[inline(always)]
    pub fn name_key(&self) -> usize {
        // SAFETY: blpapi_Message_messageType returns a valid interned Name pointer.
        unsafe { ffi::blpapi_Message_messageType(self.ptr) as usize }
    }

    /// Check if the message type matches a pre-interned `Name`
    /// (O(1) pointer comparison, no allocation).
    #[inline(always)]
    pub fn name_eq(&self, other: &Name) -> bool {
        // SAFETY: blpapi_Message_messageType returns a valid interned Name pointer.
        let ptr = unsafe { ffi::blpapi_Message_messageType(self.ptr) };
        ptr == other.as_ptr()
    }

    /// Get the time this message was received by the SDK, as microseconds since Unix epoch.
    ///
    /// Returns `None` if receive-time recording was not enabled via
    /// `SessionOptions::set_record_subscription_receive_times(true)`.
    ///
    /// This is more accurate than `SystemTime::now()` because it records the time
    /// at the SDK's network layer, before any queuing or processing delays.
    #[inline]
    pub fn time_received_us(&self) -> Option<i64> {
        let mut tp = MaybeUninit::<ffi::blpapi_TimePoint_t>::uninit();
        // SAFETY: self.ptr is valid for the lifetime 'a. timeReceived writes to tp on success.
        let rc = unsafe { ffi::blpapi_Message_timeReceived(self.ptr as *const _, tp.as_mut_ptr()) };
        if rc != 0 {
            return None;
        }
        let tp = unsafe { tp.assume_init() };

        let mut dt = MaybeUninit::<ffi::blpapi_HighPrecisionDatetime_t>::uninit();
        // SAFETY: tp is initialized above. offset=0 gives UTC datetime.
        let rc =
            unsafe { ffi::blpapi_HighPrecisionDatetime_fromTimePoint(dt.as_mut_ptr(), &tp, 0) };
        if rc != 0 {
            return None;
        }

        let hpdt = crate::HighPrecisionDatetime(unsafe { dt.assume_init() });
        Some(hpdt.to_micros())
    }

    /// Get raw pointer for FFI calls (internal use).
    ///
    /// This is used internally by other xbbg-core types that need to call
    /// Bloomberg C API functions.
    #[inline(always)]
    #[allow(dead_code)] // Used in integration, not unit tests
    pub(crate) fn as_ptr(&self) -> *mut ffi::blpapi_Message_t {
        self.ptr
    }

    /// Get the number of correlation IDs on this message.
    ///
    /// Most messages have exactly one correlation ID, but multi-correlation
    /// responses (MCM) can have multiple.
    ///
    /// # Performance
    /// This is a hot path method - returns immediately with no allocation.
    #[inline(always)]
    pub fn num_correlation_ids(&self) -> usize {
        // SAFETY: self.ptr is valid for the lifetime 'a
        let count = unsafe { ffi::blpapi_Message_numCorrelationIds(self.ptr) };
        count.max(0) as usize
    }

    /// Get the correlation ID at the specified index.
    ///
    /// Returns `None` if the index is out of bounds.
    ///
    /// # Arguments
    /// * `index` - Zero-based index of the correlation ID
    ///
    /// # Performance
    /// This is a hot path method - minimal allocation (just the CorrelationId).
    /// When looping over all IDs, prefer [`Message::correlation_ids`], which
    /// reads the count once instead of re-checking per index.
    #[inline]
    pub fn correlation_id(&self, index: usize) -> Option<CorrelationId> {
        if index >= self.num_correlation_ids() {
            return None;
        }

        // SAFETY: We've verified index is in bounds, and self.ptr is valid.
        // blpapi_Message_correlationId returns the CorrelationId by value.
        unsafe {
            let cid = ffi::blpapi_Message_correlationId(self.ptr, index);
            Some(CorrelationId::from_ffi(&cid))
        }
    }

    /// Iterator over all correlation IDs on this message.
    ///
    /// Reads `numCorrelationIds` once; each step is a single
    /// `blpapi_Message_correlationId` call (no per-item bounds re-check).
    #[inline]
    pub fn correlation_ids(&self) -> CorrelationIdIter<'_, 'a> {
        CorrelationIdIter {
            msg: self,
            idx: 0,
            len: self.num_correlation_ids(),
        }
    }
}

/// Iterator over the correlation IDs of a [`Message`].
///
/// Created by [`Message::correlation_ids`]. The count is read once at
/// construction; iteration performs one FFI call per ID.
pub struct CorrelationIdIter<'m, 'a> {
    msg: &'m Message<'a>,
    idx: usize,
    len: usize,
}

impl Iterator for CorrelationIdIter<'_, '_> {
    type Item = CorrelationId;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.len {
            let i = self.idx;
            self.idx += 1;
            // SAFETY: i < len == numCorrelationIds, and msg.ptr is valid.
            // blpapi_Message_correlationId returns the CorrelationId by value.
            unsafe {
                let cid = ffi::blpapi_Message_correlationId(self.msg.ptr, i);
                Some(CorrelationId::from_ffi(&cid))
            }
        } else {
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len - self.idx;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for CorrelationIdIter<'_, '_> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_size() {
        // Message should be pointer-sized (transparent wrapper)
        assert_eq!(
            std::mem::size_of::<Message>(),
            std::mem::size_of::<*mut ()>()
        );
    }

    #[test]
    fn test_message_alignment() {
        // Message should have pointer alignment
        assert_eq!(
            std::mem::align_of::<Message>(),
            std::mem::align_of::<*mut ()>()
        );
    }
}
