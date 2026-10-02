//! Event type for Bloomberg BLPAPI
//!
//! Events are the primary containers for Bloomberg responses.
//! Each event contains one or more messages with data.
//!
//! **Ownership**: Events OWN their event pointer and release it on Drop.
//! Messages borrow the [`MessageIterator`] that yields them and are valid only
//! until it advances or is dropped.

use crate::{ffi, Message};
use std::marker::PhantomData;
use std::rc::Rc;

/// Bloomberg event wrapper.
///
/// OWNS the event pointer. When dropped, releases the event via `blpapi_Event_release`.
/// NOT thread-safe - must be consumed on receiving thread.
///
/// # Ownership
/// Event owns the `blpapi_Event_t` pointer. Messages read through
/// [`messages()`](Self::messages) borrow that iterator, not the Event.
///
/// # Thread Safety
/// Events are `!Send + !Sync` because:
/// - Bloomberg's API is not thread-safe
/// - Events must be processed on the thread that received them
/// - Uses `PhantomData<Rc<()>>` to enforce this at compile time
///
/// # Performance
/// All methods are `#[inline]` or `#[inline(always)]` for zero-cost abstraction.
#[repr(transparent)]
pub struct Event {
    ptr: *mut ffi::blpapi_Event_t,
    _marker: PhantomData<Rc<()>>, // Makes !Send + !Sync
}

impl Event {
    /// Construct from raw pointer (internal use only).
    ///
    /// # Safety
    /// Caller must ensure:
    /// - `ptr` is a valid `blpapi_Event_t` pointer
    /// - Caller transfers ownership to this Event
    /// - The pointer will not be used elsewhere after this call
    #[inline]
    pub(crate) unsafe fn from_raw(ptr: *mut ffi::blpapi_Event_t) -> Self {
        Self {
            ptr,
            _marker: PhantomData,
        }
    }

    /// Event type.
    ///
    /// Returns the type of this event (Response, PartialResponse, etc.).
    /// Use this to determine how to process the event.
    ///
    /// # Performance
    /// This is a hot path method - returns immediately with no allocation.
    #[inline(always)]
    pub fn event_type(&self) -> EventType {
        // SAFETY: blpapi_Event_eventType returns integer event type.
        // The pointer is valid because we own it and haven't dropped yet.
        let ty = unsafe { ffi::blpapi_Event_eventType(self.ptr) };
        EventType::from_raw(ty)
    }

    /// Cursor over the messages in this event.
    ///
    /// Each message borrows the returned [`MessageIterator`] and is valid only
    /// until the next [`MessageIterator::next`] call.
    ///
    /// # Performance
    /// Zero allocation - creates a Bloomberg message iterator that
    /// yields messages on demand.
    ///
    /// # Example
    /// ```ignore
    /// let mut messages = event.messages();
    /// while let Some(msg) = messages.next() {
    ///     let root = msg.elements();
    ///     // Process message...
    /// }
    /// ```
    #[inline]
    pub fn messages(&self) -> MessageIterator<'_> {
        // SAFETY: blpapi_MessageIterator_create returns valid iterator or null.
        // The iterator is valid for the lifetime of the event.
        // We pass ownership of the iterator pointer to MessageIterator.
        let iter_ptr = unsafe { ffi::blpapi_MessageIterator_create(self.ptr) };
        MessageIterator {
            ptr: iter_ptr,
            _life: PhantomData,
            _marker: PhantomData,
        }
    }

    /// Get raw pointer (internal use).
    ///
    /// This is used internally by other xbbg-core types that need to call
    /// Bloomberg C API functions.
    #[inline(always)]
    #[allow(dead_code)] // Used in integration, not unit tests
    pub(crate) fn as_ptr(&self) -> *mut ffi::blpapi_Event_t {
        self.ptr
    }
}

impl Drop for Event {
    fn drop(&mut self) {
        // SAFETY: We own this pointer, and Drop is called exactly once.
        // blpapi_Event_release decrements the refcount and frees if zero.
        // The pointer is valid because we haven't released it yet.
        unsafe {
            ffi::blpapi_Event_release(self.ptr);
        }
    }
}

/// Bloomberg event types.
///
/// These correspond to the event type constants in Bloomberg's C++ SDK.
/// Use `event.event_type()` to determine how to process an event.
///
/// # Common Types
/// - `Response`: Final response to a request
/// - `PartialResponse`: Intermediate response (more data coming)
/// - `SubscriptionData`: Real-time subscription update
/// - `SessionStatus`: Session state change
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum EventType {
    /// Administrative event
    Admin = 1,
    /// Session status change
    SessionStatus = 2,
    /// Subscription status change
    SubscriptionStatus = 3,
    /// Request status update
    RequestStatus = 4,
    /// Final response to request
    Response = 5,
    /// Partial response (more data coming)
    PartialResponse = 6,
    /// Real-time subscription data
    SubscriptionData = 8,
    /// Service status change
    ServiceStatus = 9,
    /// Request timeout
    Timeout = 10,
    /// Authorization status
    AuthorizationStatus = 11,
    /// Resolution status
    ResolutionStatus = 12,
    /// Topic status
    TopicStatus = 13,
    /// Token status
    TokenStatus = 14,
    /// Request event
    Request = 15,
    /// Unknown event type (with raw value)
    Unknown(i32),
}

impl EventType {
    /// Convert from raw Bloomberg integer.
    ///
    /// Maps Bloomberg's event type constants to our enum.
    /// Unknown values are wrapped in `Unknown(i32)`.
    #[inline]
    pub fn from_raw(v: i32) -> Self {
        match v {
            1 => Self::Admin,
            2 => Self::SessionStatus,
            3 => Self::SubscriptionStatus,
            4 => Self::RequestStatus,
            5 => Self::Response,
            6 => Self::PartialResponse,
            8 => Self::SubscriptionData,
            9 => Self::ServiceStatus,
            10 => Self::Timeout,
            11 => Self::AuthorizationStatus,
            12 => Self::ResolutionStatus,
            13 => Self::TopicStatus,
            14 => Self::TokenStatus,
            15 => Self::Request,
            _ => Self::Unknown(v),
        }
    }

    /// Convert to raw Bloomberg integer.
    ///
    /// Returns the integer value that Bloomberg's C API uses.
    #[inline]
    pub fn to_raw(&self) -> i32 {
        match self {
            Self::Admin => 1,
            Self::SessionStatus => 2,
            Self::SubscriptionStatus => 3,
            Self::RequestStatus => 4,
            Self::Response => 5,
            Self::PartialResponse => 6,
            Self::SubscriptionData => 8,
            Self::ServiceStatus => 9,
            Self::Timeout => 10,
            Self::AuthorizationStatus => 11,
            Self::ResolutionStatus => 12,
            Self::TopicStatus => 13,
            Self::TokenStatus => 14,
            Self::Request => 15,
            Self::Unknown(v) => *v,
        }
    }
}

/// Cursor over the messages in an event.
///
/// Created by [`Event::messages`]. Bloomberg keeps a message valid only until
/// its iterator advances or is destroyed, so each [`Message`] returned by
/// [`next`](Self::next) borrows this cursor, and the compiler rejects any use
/// after the following `next()` call or after the cursor is dropped. That
/// borrow is why this is not an [`Iterator`]; loop with `while let`:
///
/// ```no_run
/// # fn process(event: &xbbg_core::Event) {
/// let mut messages = event.messages();
/// while let Some(msg) = messages.next() {
///     println!("{}", msg.type_str());
/// }
/// # }
/// ```
///
/// A message cannot be used after the cursor advances:
///
/// ```compile_fail,E0499
/// # fn process(event: &xbbg_core::Event) {
/// let mut messages = event.messages();
/// let first = messages.next().unwrap();
/// let _second = messages.next();
/// let _root = first.elements();
/// # }
/// ```
///
/// or after the cursor is dropped:
///
/// ```compile_fail,E0716
/// # fn process(event: &xbbg_core::Event) {
/// let first = event.messages().next().unwrap();
/// let _root = first.elements();
/// # }
/// ```
///
/// # Thread Safety
/// MessageIterator is `!Send + !Sync` because:
/// - Bloomberg's API is not thread-safe
/// - Must be consumed on the same thread as the Event
pub struct MessageIterator<'a> {
    ptr: *mut ffi::blpapi_MessageIterator_t,
    _life: PhantomData<&'a Event>,
    _marker: PhantomData<Rc<()>>, // Makes !Send + !Sync
}

impl MessageIterator<'_> {
    /// Advance to the next message, or `None` once the event has no more.
    ///
    /// The message borrows this cursor, so it must be dropped before the next
    /// call.
    #[expect(
        clippy::should_implement_trait,
        reason = "Iterator::next cannot return a message that borrows the iterator"
    )]
    pub fn next(&mut self) -> Option<Message<'_>> {
        let mut msg_ptr: *mut ffi::blpapi_Message_t = std::ptr::null_mut();
        // SAFETY: `self.ptr` came from blpapi_MessageIterator_create and is
        // destroyed only in Drop. The call returns 0 and writes a message
        // pointer when another message is available, non-zero otherwise.
        let rc = unsafe { ffi::blpapi_MessageIterator_next(self.ptr, &mut msg_ptr) };
        if rc == 0 && !msg_ptr.is_null() {
            // SAFETY: Bloomberg keeps this message valid until the iterator
            // advances or is destroyed (blpapi_event.h, MessageIterator::message).
            // The returned Message borrows `self` mutably, so neither can
            // happen while it is alive.
            Some(unsafe { Message::from_raw(msg_ptr) })
        } else {
            None
        }
    }
}

impl<'a> Drop for MessageIterator<'a> {
    fn drop(&mut self) {
        // SAFETY: We own this iterator pointer, and Drop is called exactly once.
        // blpapi_MessageIterator_destroy frees the iterator.
        // The pointer is valid because we haven't destroyed it yet.
        unsafe {
            ffi::blpapi_MessageIterator_destroy(self.ptr);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_from_raw_known() {
        assert_eq!(EventType::from_raw(1), EventType::Admin);
        assert_eq!(EventType::from_raw(2), EventType::SessionStatus);
        assert_eq!(EventType::from_raw(5), EventType::Response);
        assert_eq!(EventType::from_raw(6), EventType::PartialResponse);
        assert_eq!(EventType::from_raw(8), EventType::SubscriptionData);
        assert_eq!(EventType::from_raw(10), EventType::Timeout);
        assert_eq!(EventType::from_raw(15), EventType::Request);
    }

    #[test]
    fn test_event_type_from_raw_unknown() {
        assert_eq!(EventType::from_raw(999), EventType::Unknown(999));
        assert_eq!(EventType::from_raw(-1), EventType::Unknown(-1));
        assert_eq!(EventType::from_raw(100), EventType::Unknown(100));
    }

    #[test]
    fn test_event_type_to_raw() {
        assert_eq!(EventType::Response.to_raw(), 5);
        assert_eq!(EventType::PartialResponse.to_raw(), 6);
        assert_eq!(EventType::SubscriptionData.to_raw(), 8);
        assert_eq!(EventType::Unknown(999).to_raw(), 999);
    }

    #[test]
    fn test_event_type_roundtrip() {
        for i in [1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15] {
            let ty = EventType::from_raw(i);
            assert_eq!(ty.to_raw(), i);
        }
    }

    #[test]
    fn test_event_size() {
        // Event should be pointer-sized (transparent wrapper)
        assert_eq!(std::mem::size_of::<Event>(), std::mem::size_of::<*mut ()>());
    }

    #[test]
    fn test_event_alignment() {
        // Event should have pointer alignment
        assert_eq!(
            std::mem::align_of::<Event>(),
            std::mem::align_of::<*mut ()>()
        );
    }

    #[test]
    fn test_message_iterator_size() {
        // MessageIterator should be pointer-sized
        assert_eq!(
            std::mem::size_of::<MessageIterator>(),
            std::mem::size_of::<*mut ()>()
        );
    }
}
