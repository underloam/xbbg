//! Asynchronous (event-handler) Bloomberg session.
//!
//! An [`AsyncSession`] is constructed with a Rust callback; the BLPAPI SDK
//! delivers every event for the session by invoking that callback from an SDK
//! dispatcher thread (one dedicated thread per session by default,
//! `blpapi_session.h:549-551`). There is no event queue to poll —
//! `nextEvent` is not available in this mode and throws on the C++ side
//! (`blpapi_session.h:635-636`).
//!
//! # Threading model
//!
//! Unlike the synchronous [`Session`](crate::Session) — whose API calls are
//! thread-safe only when made on the thread that pumps `nextEvent`
//! (`blpapi_session.h:496-497`) — an asynchronous session has no pump thread,
//! and the SDK documents this mode for cross-thread use: callbacks arrive
//! from SDK threads concurrently with API calls, and may even be processed
//! before the call that generated them returns (`blpapi_session.h:484-495`).
//! `AsyncSession` is therefore `Send + Sync` and methods take `&self`.
//!
//! # Callback discipline
//!
//! - The handler runs on an SDK dispatcher thread. It must not call
//!   [`AsyncSession::stop`] (vendor-documented deadlock,
//!   `blpapi_session.h:606-607`) and must not panic: release builds use
//!   `panic = "abort"`, and in unwind builds rustc aborts on a panic crossing
//!   the `extern "C"` trampoline. Keep handler bodies panic-free.
//! - Bloomberg recommends caller-supplied correlation IDs in this mode
//!   because events can outrun the submitting call (`blpapi_session.h:490-495`).
//!   Register request state *before* calling [`AsyncSession::send_request`].

use std::os::raw::c_void;

use crate::correlation::CorrelationId;
use crate::errors::{BlpError, Result};
use crate::event::Event;
use crate::options::SessionOptions;
use crate::request::Request;
use crate::service::Service;
use crate::session_operations::SessionOperations;

/// Stable-address holder for the user handler; the SDK keeps a pointer to
/// this allocation for the lifetime of the session.
struct HandlerShared {
    f: Box<dyn Fn(Event) + Send + Sync + 'static>,
}

/// C trampoline registered with `blpapi_Session_create`.
///
/// # Safety
/// `user_data` must point at a live `HandlerShared`. `AsyncSession` guarantees
/// this by keeping the box alive until after `blpapi_Session_stop` has
/// returned (which blocks until all in-flight callbacks complete,
/// `blpapi_session.h:599-609`) and the session is destroyed.
unsafe extern "C" fn event_trampoline(
    event: *mut crate::ffi::blpapi_Event_t,
    _session: *mut crate::ffi::blpapi_Session_t,
    user_data: *mut c_void,
) {
    if event.is_null() || user_data.is_null() {
        return;
    }
    // SAFETY: null was rejected above, and the function contract guarantees
    // `user_data` points at the live `HandlerShared` owned by `AsyncSession`.
    let shared = unsafe { &*(user_data as *const HandlerShared) };
    // SAFETY: the SDK transfers ownership of the event to the handler (the
    // C++ adapter wraps it in `Event`, whose destructor releases it —
    // blpapi_session.h:1171-1176). `Event::from_raw` models the same.
    let event = unsafe { Event::from_raw(event) };
    (shared.f)(event);
}

/// An asynchronous Bloomberg session driven by an event-handler callback.
///
/// See the module docs for the threading and callback contracts.
pub struct AsyncSession {
    ptr: *mut crate::ffi::blpapi_Session_t,
    /// Kept alive for the SDK's `userData` pointer until after stop and destroy.
    _handler: Box<HandlerShared>,
}

// SAFETY: asynchronous sessions are the SDK's documented multi-threaded mode:
// the same-thread restriction is scoped to synchronous sessions
// (blpapi_session.h:496-497), callbacks are delivered from SDK-owned threads
// concurrently with API calls (blpapi_session.h:484-495), and all handle
// state is managed inside the SDK. The handler is required to be
// `Send + Sync` at construction.
unsafe impl Send for AsyncSession {}
unsafe impl Sync for AsyncSession {}

impl AsyncSession {
    fn operations(&self) -> SessionOperations<'_> {
        // SAFETY: the borrow keeps the session alive; asynchronous sessions
        // permit these operations from concurrent calling threads.
        unsafe { SessionOperations::new(&self.ptr) }
    }

    /// Create an asynchronous session delivering events to `handler`.
    ///
    /// The session is created but not started; call [`AsyncSession::start`].
    /// `handler` runs on an SDK dispatcher thread — see the module docs for
    /// the discipline it must follow.
    pub fn new(
        options: &SessionOptions,
        handler: impl Fn(Event) + Send + Sync + 'static,
    ) -> Result<Self> {
        let shared = Box::new(HandlerShared {
            f: Box::new(handler),
        });
        let user_data = &*shared as *const HandlerShared as *mut c_void;

        // SAFETY: options.as_raw() is valid; the trampoline matches
        // blpapi_EventHandler_t; user_data outlives the session (field order
        // + Drop impl below). Null dispatcher = SDK-owned single dispatcher
        // thread for this session.
        let ptr = unsafe {
            crate::ffi::blpapi_Session_create(
                options.as_raw(),
                Some(event_trampoline),
                std::ptr::null_mut(),
                user_data,
            )
        };

        if ptr.is_null() {
            return Err(BlpError::SessionStart {
                source: None,
                label: None,
            });
        }

        Ok(Self {
            ptr,
            _handler: shared,
        })
    }

    /// Start the session, blocking until it has started or failed to start
    /// (`blpapi_session.h:577-586`).
    ///
    /// Status events (`SessionStarted` / `SessionStartupFailure`) are also
    /// delivered to the handler and may arrive *before* this returns; track
    /// startup detail there if needed.
    pub fn start(&self) -> Result<()> {
        // SAFETY: valid session pointer.
        let rc = unsafe { crate::ffi::blpapi_Session_start(self.ptr) };
        if rc != 0 {
            return Err(BlpError::SessionStart {
                source: None,
                label: None,
            });
        }
        Ok(())
    }

    /// Stop the session, blocking until all in-flight handler callbacks have
    /// completed; no callbacks occur afterwards (`blpapi_session.h:599-609`).
    ///
    /// # Deadlock
    /// Never call from within the event handler.
    pub fn stop(&self) {
        // SAFETY: valid session pointer.
        unsafe {
            crate::ffi::blpapi_Session_stop(self.ptr);
        }
    }

    /// Begin stopping the session without waiting (`blpapi_session.h:611-621`).
    pub fn stop_async(&self) {
        // SAFETY: valid session pointer.
        unsafe {
            crate::ffi::blpapi_Session_stopAsync(self.ptr);
        }
    }

    /// Open a service, blocking until it is opened or fails.
    ///
    /// Unlike on a synchronous session this does not stall event delivery —
    /// events keep flowing on the SDK dispatcher thread. Used for warmup at
    /// session construction; prefer [`AsyncSession::open_service_async`] on
    /// latency-sensitive paths.
    pub fn open_service(&self, name: &str) -> Result<()> {
        self.operations().open_service(name)
    }

    /// Open a service asynchronously; the `ServiceOpened` /
    /// `ServiceOpenFailure` reply reaches the handler tagged with the
    /// returned correlation ID.
    pub fn open_service_async(&self, name: &str, cid: &CorrelationId) -> Result<CorrelationId> {
        self.operations().open_service_async(name, cid)
    }

    /// Get a service handle. The service must already be open; the handle
    /// borrows this session's reference and cannot outlive it.
    pub fn get_service(&self, name: &str) -> Result<Service<'_>> {
        self.operations().get_service(name)
    }

    /// Send a request. Register any state keyed on `cid` *before* calling —
    /// the response can reach the handler before this returns.
    pub fn send_request(
        &self,
        req: &Request,
        cid: Option<&CorrelationId>,
    ) -> Result<CorrelationId> {
        self.send_request_with_label(req, cid, None)
    }

    /// Send a request with an optional diagnostics label.
    pub fn send_request_with_label(
        &self,
        req: &Request,
        cid: Option<&CorrelationId>,
        label: Option<&str>,
    ) -> Result<CorrelationId> {
        self.operations().send_request(req, None, cid, label)
    }

    /// Cancel an in-flight correlation ID.
    pub fn cancel(&self, cid: &CorrelationId) -> Result<()> {
        self.operations().cancel(cid)
    }

    /// Begin subscriptions for every entry in `subs`
    /// (`blpapi_session.h`: subscriptions are legal on asynchronous sessions;
    /// data and status events reach the handler tagged with each entry's
    /// correlation ID).
    ///
    /// Register per-CID state *before* calling — `SubscriptionStatus` /
    /// `SubscriptionData` events can reach the handler before this returns.
    pub fn subscribe(&self, subs: &crate::SubscriptionList, label: Option<&str>) -> Result<()> {
        self.operations()
            .subscribe(subs, None, label, "subscription label")
    }

    /// Update existing subscriptions, matched by their correlation IDs.
    ///
    /// Each entry supplies the complete replacement field list and options.
    /// Update per-CID state before calling: status and repaint data may reach
    /// the handler before this returns, as with [`AsyncSession::subscribe`].
    pub fn resubscribe(&self, subs: &crate::SubscriptionList, label: Option<&str>) -> Result<()> {
        self.operations()
            .resubscribe(subs, label, "subscription label")
    }

    /// Cancel the subscriptions in `subs`; entries are matched by
    /// correlation ID. Termination is confirmed to the handler via
    /// `SubscriptionTerminated` / `SubscriptionFailure` status messages.
    pub fn unsubscribe(&self, subs: &crate::SubscriptionList) -> Result<()> {
        self.operations().unsubscribe(subs)
    }

    /// Begin asynchronous authorization of an identity described by
    /// `auth_options` (blpapi_abstractsession.h:540-560). One or more
    /// `AUTHORIZATION_STATUS` events tagged with `cid` reach the handler:
    /// `AuthorizationSuccess` when the identity is ready (retrieve it with
    /// [`AsyncSession::authorized_identity`]), `AuthorizationFailure` on
    /// rejection, and `AuthorizationRevoked` if entitlements are later
    /// withdrawn.
    pub fn generate_authorized_identity_async(
        &self,
        auth_options: &crate::auth::AuthOptions,
        cid: &CorrelationId,
    ) -> Result<()> {
        let mut cid_ffi = cid.to_ffi();
        // SAFETY: valid session pointer (getAbstractSession is a straight
        // handle projection, never null for a live session —
        // blpapi_session.h:1197); auth options pointer is valid for the call.
        let rc = unsafe {
            let abstract_session = crate::ffi::blpapi_Session_getAbstractSession(self.ptr);
            crate::ffi::blpapi_AbstractSession_generateAuthorizedIdentityAsync(
                abstract_session,
                auth_options.as_ptr(),
                &mut cid_ffi,
            )
        };
        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("generateAuthorizedIdentityAsync failed with rc={rc}"),
            });
        }
        Ok(())
    }

    /// Retrieve the authorized identity produced by a completed
    /// [`AsyncSession::generate_authorized_identity_async`] call.
    ///
    /// Returns an owned, reference-counted handle (the C++ wrapper adopts it
    /// into `Identity(identity)`, single release — blpapi_abstractsession.h:749-753);
    /// each call materializes a fresh handle, so use-and-drop on the calling
    /// thread is safe. Errors until `AuthorizationSuccess` has been delivered
    /// for `cid`.
    pub fn authorized_identity(&self, cid: &CorrelationId) -> Result<crate::Identity> {
        let cid_ffi = cid.to_ffi();
        let mut identity_ptr: *mut crate::ffi::blpapi_Identity_t = std::ptr::null_mut();
        // SAFETY: valid session pointer; cid and out-param are valid for the
        // call; the returned handle is owned by the new `Identity` (released
        // exactly once in its Drop).
        let rc = unsafe {
            let abstract_session = crate::ffi::blpapi_Session_getAbstractSession(self.ptr);
            crate::ffi::blpapi_AbstractSession_getAuthorizedIdentity(
                abstract_session,
                &cid_ffi,
                &mut identity_ptr,
            )
        };
        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("getAuthorizedIdentity failed with rc={rc}"),
            });
        }
        crate::Identity::from_raw(identity_ptr)
    }

    /// Request a token for the session's effective authentication user
    /// (default: OS logon). The outcome arrives at the handler as a
    /// `TOKEN_STATUS` event tagged with `cid`: `TokenGenerationSuccess`
    /// carries a `token` element, `TokenGenerationFailure` a `reason`.
    pub fn generate_token(&self, cid: &CorrelationId) -> Result<()> {
        self.operations().generate_token(Some(cid)).map(|_| ())
    }

    /// Create a fresh, not-yet-authorized identity handle for this session.
    /// Authorize it with [`AsyncSession::send_authorization_request`].
    pub fn create_identity(&self) -> Result<crate::Identity> {
        self.operations().create_identity()
    }

    /// Send an `AuthorizationRequest` (from `//blp/apiauth`) that authorizes
    /// `identity` in place. The outcome reaches the handler tagged with
    /// `cid`: an `AuthorizationSuccess` or `AuthorizationFailure` message
    /// (RESPONSE / PARTIAL_RESPONSE / AUTHORIZATION_STATUS event).
    pub fn send_authorization_request(
        &self,
        request: &Request,
        identity: &mut crate::Identity,
        cid: &CorrelationId,
    ) -> Result<()> {
        self.operations()
            .send_authorization_request(request, identity, Some(cid))
            .map(|_| ())
    }
}

impl Drop for AsyncSession {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: stop() blocks until in-flight callbacks complete and
            // guarantees no further callbacks (blpapi_session.h:599-609), so
            // destroying the session and then freeing the handler box (field
            // drop after this body) cannot race the trampoline.
            unsafe {
                crate::ffi::blpapi_Session_stop(self.ptr);
                crate::ffi::blpapi_Session_destroy(self.ptr);
            }
            self.ptr = std::ptr::null_mut();
        }
    }
}
