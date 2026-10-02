//! Bloomberg session management

use std::ffi::CString;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::correlation::CorrelationId;
use crate::errors::{BlpError, Result};
use crate::event::Event;
use crate::identity::Identity;
use crate::message::Message;
use crate::request::Request;
use crate::service::Service;
use crate::subscription::SubscriptionList;

// Re-export SessionOptions from options module
pub use crate::options::SessionOptions;

/// Bloomberg session for making requests and receiving data.
///
/// A `Session` represents a connection to the Bloomberg API. The local SDK
/// headers document synchronous sessions as a same-thread polling model, so
/// this wrapper is intentionally neither `Send` nor `Sync`.
///
/// # Examples
///
/// ```ignore
/// use xbbg_core::{Session, SessionOptions, EventType, Name};
///
/// // Pre-intern names (do once at setup)
/// let securities = Name::get_or_intern("securities");
/// let fields = Name::get_or_intern("fields");
///
/// // Create and start session
/// let mut opts = SessionOptions::new()?;
/// opts.set_server_host("localhost")?;
/// opts.set_server_port(8194);
///
/// let sess = Session::new(&opts)?;
/// sess.start()?;
///
/// // Wait for SessionStarted
/// loop {
///     if let Ok(ev) = sess.next_event(Some(5000)) {
///         if ev.event_type() == EventType::SessionStatus {
///             break;
///         }
///     }
/// }
///
/// // Open service and make request
/// sess.open_service("//blp/refdata")?;
/// let svc = sess.get_service("//blp/refdata")?;
/// let mut req = svc.create_request("ReferenceDataRequest")?;
///
/// req.append_string(&securities, "IBM US Equity")?;
/// req.append_string(&fields, "PX_LAST")?;
///
/// sess.send_request(&req, None, None)?;
///
/// // Process response
/// loop {
///     if let Ok(ev) = sess.next_event(Some(5000)) {
///         if ev.event_type() == EventType::Response {
///             let mut messages = ev.messages();
///             while let Some(msg) = messages.next() {
///                 // Extract data...
///             }
///             break;
///         }
///     }
/// }
///
/// sess.stop();
/// ```
///
/// # Threading Model
/// This synchronous wrapper must be used from the thread that owns it. Create
/// one session per worker thread instead of sharing a session across threads.
pub struct Session {
    ptr: *mut crate::ffi::blpapi_Session_t,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl Session {
    const STARTUP_POLL_TIMEOUT_MS: u32 = 250;

    /// Create a new session with the given options.
    ///
    /// Creates a session but does not start it. Call `start()` to initiate the connection.
    ///
    /// # Arguments
    /// * `options` - Session configuration options
    ///
    /// # Returns
    /// A new Session on success, or an error if creation fails
    pub fn new(options: &SessionOptions) -> Result<Self> {
        // SAFETY: We're calling the Bloomberg API with valid pointers
        // - options.as_raw() is guaranteed valid by SessionOptions
        // - handler, dispatcher, and eventQueue are None/null (synchronous mode)
        let ptr = unsafe {
            crate::ffi::blpapi_Session_create(
                options.as_raw(),
                None,                 // handler (None = synchronous mode)
                std::ptr::null_mut(), // dispatcher
                std::ptr::null_mut(), // eventQueue
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
            _not_send_sync: PhantomData,
        })
    }

    /// Start the session.
    ///
    /// This initiates the connection to the Bloomberg API. You should wait for
    /// a `SessionStatus` event with `SessionStarted` message before making requests.
    ///
    /// # Returns
    /// Ok(()) on success, Err on failure
    pub fn start(&self) -> Result<()> {
        // SAFETY: We're calling the Bloomberg API with a valid pointer
        let rc = unsafe { crate::ffi::blpapi_Session_start(self.ptr) };

        if rc != 0 {
            return Err(BlpError::SessionStart {
                source: None,
                label: None,
            });
        }

        Ok(())
    }

    pub fn wait_until_started(&self, timeout_ms: u32) -> Result<()> {
        let deadline = Instant::now() + Duration::from_millis(u64::from(timeout_ms));

        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(BlpError::Timeout);
            }

            let remaining = deadline.saturating_duration_since(now);
            let poll_timeout = remaining
                .min(Duration::from_millis(u64::from(
                    Self::STARTUP_POLL_TIMEOUT_MS,
                )))
                .as_millis() as u32;

            let poll_timeout = poll_timeout.max(1);
            let event = self.next_event(Some(poll_timeout))?;
            let mut saw_session_started = false;
            let mut messages = event.messages();
            while let Some(msg) = messages.next() {
                match msg.message_type().as_str() {
                    "SessionStarted" => saw_session_started = true,
                    "SessionStartupFailure" => {
                        return Err(startup_error_from_message("session startup failure", &msg));
                    }
                    "SessionTerminated" => {
                        return Err(startup_error_from_message(
                            "session terminated during startup",
                            &msg,
                        ));
                    }
                    "AuthorizationFailure" => {
                        return Err(startup_error_from_message(
                            "session identity authorization failed",
                            &msg,
                        ));
                    }
                    "AuthorizationRevoked" => {
                        return Err(startup_error_from_message(
                            "session identity authorization revoked",
                            &msg,
                        ));
                    }
                    _ => {}
                }
            }
            if saw_session_started {
                return Ok(());
            }
        }
    }

    pub fn start_and_wait(&self, timeout_ms: u32) -> Result<()> {
        self.start()?;
        self.wait_until_started(timeout_ms)
    }

    /// Stop the session.
    ///
    /// This closes the connection to the Bloomberg API. After calling stop(),
    /// the session cannot be restarted. Always call this before dropping the session
    /// to ensure clean shutdown.
    pub fn stop(&self) {
        // SAFETY: We're calling the Bloomberg API with a valid pointer
        unsafe {
            crate::ffi::blpapi_Session_stop(self.ptr);
        }
    }

    /// Wait for the next event with an optional timeout.
    ///
    /// This is the primary method for receiving data from Bloomberg. Blocks until
    /// an event is available or the timeout expires.
    ///
    /// # Arguments
    /// * `timeout_ms` - Optional timeout in milliseconds. None means wait indefinitely.
    ///
    /// # Returns
    /// The next Event. When the timeout expires Bloomberg returns an event of
    /// type [`EventType::Timeout`](crate::EventType::Timeout), not an error;
    /// `Err` means the call itself failed (e.g. session in an invalid state).
    pub fn next_event(&self, timeout_ms: Option<u32>) -> Result<Event> {
        let mut event_ptr: *mut crate::ffi::blpapi_Event_t = std::ptr::null_mut();

        // SAFETY: We're calling the Bloomberg API with valid pointers
        let rc = unsafe {
            crate::ffi::blpapi_Session_nextEvent(self.ptr, &mut event_ptr, timeout_ms.unwrap_or(0))
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("blpapi_Session_nextEvent failed: rc={rc}"),
            });
        }

        if event_ptr.is_null() {
            return Err(BlpError::Internal {
                detail: "nextEvent returned null event".into(),
            });
        }

        // SAFETY: event_ptr is guaranteed non-null and valid from blpapi_Session_nextEvent
        Ok(unsafe { Event::from_raw(event_ptr) })
    }

    /// Try to get the next event without blocking.
    ///
    /// Non-blocking version of `next_event()`. Returns immediately.
    ///
    /// # Returns
    /// Some(Event) if an event is available, None if no event is ready
    pub fn try_next_event(&self) -> Option<Event> {
        let mut event_ptr: *mut crate::ffi::blpapi_Event_t = std::ptr::null_mut();

        // SAFETY: We're calling the Bloomberg API with valid pointers
        let rc = unsafe { crate::ffi::blpapi_Session_tryNextEvent(self.ptr, &mut event_ptr) };

        if rc == 0 && !event_ptr.is_null() {
            // SAFETY: event_ptr is guaranteed non-null and valid from blpapi_Session_tryNextEvent
            Some(unsafe { Event::from_raw(event_ptr) })
        } else {
            None
        }
    }

    /// Open a service (synchronous).
    ///
    /// Blocks the calling thread until the service is opened or the open fails.
    /// Per BLPAPI docs, the synchronous `openService` internally pumps the
    /// session's event queue, which means events arriving during the call are
    /// delayed until it returns. Prefer `open_service_async` inside an event
    /// loop that already has active subscriptions to avoid stalling delivery.
    ///
    /// You should still wait for a `ServiceOpened` event before calling
    /// `get_service()`.
    ///
    /// # Arguments
    /// * `name` - The service name (e.g., "//blp/refdata")
    pub fn open_service(&self, name: &str) -> Result<()> {
        let c_name = CString::new(name).map_err(|e| BlpError::InvalidArgument {
            detail: format!("invalid service name: {}", e),
        })?;

        // SAFETY: We're calling the Bloomberg API with valid pointers
        let rc = unsafe { crate::ffi::blpapi_Session_openService(self.ptr, c_name.as_ptr()) };

        if rc != 0 {
            return Err(BlpError::OpenService {
                service: name.to_string(),
                source: None,
                label: None,
            });
        }

        Ok(())
    }

    /// Open a service asynchronously.
    ///
    /// Returns immediately with the actual correlation ID the SDK will use to
    /// tag the eventual `ServiceOpened` or `ServiceOpenFailure` event. Callers
    /// can keep pulling events from `next_event` and match on the returned CID
    /// to know when the open has completed.
    ///
    /// This is the preferred form when there are already active subscriptions
    /// on the session — the synchronous `open_service` stalls delivery for
    /// hundreds of milliseconds while it blocks on the internal event pump.
    ///
    /// # Arguments
    /// * `name` - The service name (e.g., "//blp/mktdata")
    /// * `cid`  - Correlation ID to tag the reply with. Use `CorrelationId::Int`
    ///   with a value distinct from any in-flight subscription / request CID.
    pub fn open_service_async(&self, name: &str, cid: &CorrelationId) -> Result<CorrelationId> {
        let c_name = CString::new(name).map_err(|e| BlpError::InvalidArgument {
            detail: format!("invalid service name: {}", e),
        })?;

        let mut cid_ffi = cid.to_ffi();

        // SAFETY: Calling the Bloomberg API with valid pointers. The cid_ffi
        // out-parameter is filled with the actual CID assigned by the SDK.
        let rc = unsafe {
            crate::ffi::blpapi_Session_openServiceAsync(self.ptr, c_name.as_ptr(), &mut cid_ffi)
        };

        if rc != 0 {
            return Err(BlpError::OpenService {
                service: name.to_string(),
                source: None,
                label: None,
            });
        }

        Ok(CorrelationId::from_ffi(&cid_ffi))
    }

    /// Get a service handle.
    ///
    /// The service must have been opened first. The returned handle is borrowed
    /// from this session and cannot outlive it.
    pub fn get_service(&self, name: &str) -> Result<Service<'_>> {
        let c_name = CString::new(name).map_err(|e| BlpError::InvalidArgument {
            detail: format!("invalid service name: {}", e),
        })?;

        let mut service_ptr: *mut crate::ffi::blpapi_Service_t = std::ptr::null_mut();

        // SAFETY: We pass a valid session pointer, service name, and out-parameter.
        let rc = unsafe {
            crate::ffi::blpapi_Session_getService(self.ptr, &mut service_ptr, c_name.as_ptr())
        };

        if rc != 0 {
            return Err(BlpError::OpenService {
                service: name.to_string(),
                source: None,
                label: None,
            });
        }

        Service::from_raw(service_ptr)
    }

    /// Send a request
    ///
    /// # Arguments
    /// * `req` - The request to send
    /// * `identity` - Optional identity for authorization
    /// * `cid` - Optional correlation ID for tracking the response
    ///
    /// # Returns
    /// The actual correlation ID used on success, Err on failure
    pub fn send_request(
        &self,
        req: &Request,
        identity: Option<&Identity>,
        cid: Option<&CorrelationId>,
    ) -> Result<CorrelationId> {
        self.send_request_with_label(req, identity, cid, None)
    }

    pub fn send_request_with_label(
        &self,
        req: &Request,
        identity: Option<&Identity>,
        cid: Option<&CorrelationId>,
        label: Option<&str>,
    ) -> Result<CorrelationId> {
        // Prepare correlation ID
        let mut cid_ffi = match cid {
            Some(c) => c.to_ffi(),
            None => CorrelationId::default().to_ffi(),
        };

        // Get identity pointer
        let identity_ptr = match identity {
            Some(id) => id.as_ptr(),
            None => std::ptr::null_mut(),
        };

        let (label_ptr, label_len, _label_cstring) = match label {
            Some(value) => {
                let cstring = CString::new(value).map_err(|e| BlpError::InvalidArgument {
                    detail: format!("invalid request label: {e}"),
                })?;
                (cstring.as_ptr(), value.len() as i32, Some(cstring))
            }
            None => (std::ptr::null(), 0, None),
        };

        // SAFETY: We're calling the Bloomberg API with valid pointers
        let rc = unsafe {
            crate::ffi::blpapi_Session_sendRequest(
                self.ptr,
                req.as_ptr(),
                &mut cid_ffi,
                identity_ptr,
                std::ptr::null_mut(), // eventQueue (null = use session's queue)
                label_ptr,
                label_len,
            )
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("blpapi_Session_sendRequest failed with rc={}", rc),
            });
        }

        Ok(CorrelationId::from_ffi(&cid_ffi))
    }

    /// Subscribe to market data
    ///
    /// # Arguments
    /// * `subs` - The subscription list
    /// * `label` - Optional label for the subscription
    ///
    /// # Returns
    /// Ok(()) on success, Err on failure
    pub fn subscribe(&self, subs: &SubscriptionList, label: Option<&str>) -> Result<()> {
        let (label_ptr, label_len, _label_cstring) = match label {
            Some(l) => {
                let cs = CString::new(l).map_err(|e| BlpError::InvalidArgument {
                    detail: format!("invalid label: {}", e),
                })?;
                let len = l.len() as i32;
                (cs.as_ptr(), len, Some(cs))
            }
            None => (std::ptr::null(), 0, None),
        };

        // SAFETY: We're calling the Bloomberg API with valid pointers
        let rc = unsafe {
            crate::ffi::blpapi_Session_subscribe(
                self.ptr,
                subs.as_ptr(),
                std::ptr::null(), // identity
                label_ptr,
                label_len,
            )
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("blpapi_Session_subscribe failed with rc={}", rc),
            });
        }

        Ok(())
    }

    /// Update existing subscriptions, matched by their correlation IDs.
    ///
    /// Each entry in `subs` supplies the replacement field list and options.
    /// Use the complete desired list, not only newly added fields.
    /// `label` is an optional diagnostic label, as in [`Session::subscribe`].
    pub fn resubscribe(&self, subs: &SubscriptionList, label: Option<&str>) -> Result<()> {
        let (label_ptr, label_len, _label_cstring) = match label {
            Some(l) => {
                let cs = CString::new(l).map_err(|e| BlpError::InvalidArgument {
                    detail: format!("invalid label: {}", e),
                })?;
                (cs.as_ptr(), l.len() as i32, Some(cs))
            }
            None => (std::ptr::null(), 0, None),
        };

        // SAFETY: valid session/list pointers; the optional label remains alive
        // for the duration of the SDK call.
        let rc = unsafe {
            crate::ffi::blpapi_Session_resubscribe(self.ptr, subs.as_ptr(), label_ptr, label_len)
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("blpapi_Session_resubscribe failed with rc={}", rc),
            });
        }

        Ok(())
    }

    /// Unsubscribe from market data
    ///
    /// # Arguments
    /// * `subs` - The subscription list to unsubscribe
    ///
    /// # Returns
    /// Ok(()) on success, Err on failure
    pub fn unsubscribe(&self, subs: &SubscriptionList) -> Result<()> {
        // SAFETY: We're calling the Bloomberg API with valid pointers
        let rc = unsafe {
            crate::ffi::blpapi_Session_unsubscribe(
                self.ptr,
                subs.as_ptr(),
                std::ptr::null(), // requestLabel
                0,                // requestLabelLen
            )
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("blpapi_Session_unsubscribe failed with rc={}", rc),
            });
        }

        Ok(())
    }

    pub fn cancel(&self, cid: &CorrelationId) -> Result<()> {
        let cid_ffi = cid.to_ffi();
        let rc = unsafe {
            crate::ffi::blpapi_Session_cancel(self.ptr, &cid_ffi, 1, std::ptr::null(), 0)
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("blpapi_Session_cancel failed with rc={rc}"),
            });
        }

        Ok(())
    }

    pub fn create_identity(&self) -> Result<Identity> {
        let identity_ptr = unsafe { crate::ffi::blpapi_Session_createIdentity(self.ptr) };
        Identity::from_raw(identity_ptr)
    }

    pub fn generate_token(&self, cid: Option<&CorrelationId>) -> Result<CorrelationId> {
        let mut cid_ffi = match cid {
            Some(c) => c.to_ffi(),
            None => CorrelationId::default().to_ffi(),
        };

        let rc = unsafe {
            crate::ffi::blpapi_Session_generateToken(
                self.ptr,
                &mut cid_ffi,
                std::ptr::null_mut(), // eventQueue (null = use session's queue)
            )
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("blpapi_Session_generateToken failed with rc={rc}"),
            });
        }

        Ok(CorrelationId::from_ffi(&cid_ffi))
    }

    pub fn send_authorization_request(
        &self,
        request: &Request,
        identity: &mut Identity,
        cid: Option<&CorrelationId>,
    ) -> Result<CorrelationId> {
        let mut cid_ffi = match cid {
            Some(c) => c.to_ffi(),
            None => CorrelationId::default().to_ffi(),
        };

        let rc = unsafe {
            crate::ffi::blpapi_Session_sendAuthorizationRequest(
                self.ptr,
                request.as_ptr(),
                identity.as_ptr(),
                &mut cid_ffi,
                std::ptr::null_mut(), // eventQueue
                std::ptr::null(),     // requestLabel
                0,                    // requestLabelLen
            )
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("blpapi_Session_sendAuthorizationRequest failed with rc={rc}"),
            });
        }

        Ok(CorrelationId::from_ffi(&cid_ffi))
    }

    pub fn subscribe_with_identity(
        &self,
        subs: &SubscriptionList,
        identity: &Identity,
        label: Option<&str>,
    ) -> Result<()> {
        let (label_ptr, label_len, _label_cstring) = match label {
            Some(l) => {
                let cs = CString::new(l).map_err(|e| BlpError::InvalidArgument {
                    detail: format!("invalid label: {e}"),
                })?;
                let len = l.len() as i32;
                (cs.as_ptr(), len, Some(cs))
            }
            None => (std::ptr::null(), 0, None),
        };

        let rc = unsafe {
            crate::ffi::blpapi_Session_subscribe(
                self.ptr,
                subs.as_ptr(),
                identity.as_ptr(),
                label_ptr,
                label_len,
            )
        };

        if rc != 0 {
            return Err(BlpError::Internal {
                detail: format!("blpapi_Session_subscribe (with identity) failed with rc={rc}"),
            });
        }

        Ok(())
    }
}

fn startup_error_from_message(default_label: &str, msg: &Message<'_>) -> BlpError {
    let label = extract_reason_description(msg).unwrap_or_else(|| default_label.to_string());
    BlpError::SessionStart {
        source: None,
        label: Some(label),
    }
}

fn extract_reason_description(msg: &Message<'_>) -> Option<String> {
    let reason = msg.elements().get_by_str("reason")?;
    if let Some(description) = reason
        .get_by_str("description")
        .and_then(|value| value.get_str(0))
    {
        return Some(description.to_string());
    }
    if let Some(category) = reason
        .get_by_str("category")
        .and_then(|value| value.get_str(0))
    {
        return Some(category.to_string());
    }
    if let Some(message) = reason
        .get_by_str("message")
        .and_then(|value| value.get_str(0))
    {
        return Some(message.to_string());
    }
    None
}

impl Drop for Session {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: We're calling the Bloomberg API to clean up the session
            // First stop the session, then destroy it
            unsafe {
                crate::ffi::blpapi_Session_stop(self.ptr);
                crate::ffi::blpapi_Session_destroy(self.ptr);
            }
            self.ptr = std::ptr::null_mut();
        }
    }
}
