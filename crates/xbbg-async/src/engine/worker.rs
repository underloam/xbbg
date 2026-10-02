//! Unified request worker for all Bloomberg request types.
//!
//! Workers run Bloomberg sessions in **asynchronous (event-handler) mode**:
//! the SDK delivers events by invoking [`WorkerShared::dispatch_event`] on an
//! SDK dispatcher thread, and submitters call `sendRequest` / `cancel`
//! directly from their own threads — async sessions are the SDK's documented
//! multi-threaded mode (see [`xbbg_core::async_session`]). There is no
//! command queue and no poll loop: request dispatch latency is bounded by
//! the SDK, not by a poll quantum, and an idle worker costs zero wakeups.
//!
//! Request metadata lives in a slab shared between submitters and the
//! dispatcher callback. Slots are generation-tagged (see
//! [`super::dispatch::DispatchKey`]); mutable decoder state lives behind a
//! per-request mutex so expensive partial/final decoding never holds the slab
//! mutex. State is registered **before** `sendRequest` because in async mode
//! the response can reach the handler before the submitting call returns
//! (blpapi_session.h:490-495).
//!
//! Handled request/response patterns:
//! - Reference data (bdp)
//! - Historical data (bdh)
//! - Bulk data (bds)
//! - Intraday bars (bdib)
//! - Intraday ticks (bdtick)
//! - Field info queries

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use parking_lot::{Condvar, Mutex, RwLock};
use slab::Slab;
use tokio::sync::{mpsc, oneshot};

use xbbg_core::{
    AsyncSession, AuthConfig, BlpError, CorrelationId, EntitlementCheck, EventType, SeatType,
};

/// Max wall time we'll wait for an async open_service reply.
const SERVICE_OPEN_TIMEOUT_MS: u64 = 10_000;

/// Max wall time we'll wait for identity authorization to complete.
const IDENTITY_AUTH_TIMEOUT_MS: u64 = 10_000;

/// Authorization service used by the classic identity flow.
const APIAUTH_SERVICE: &str = "//blp/apiauth";

/// Threshold for warning about slow Bloomberg responses (30 seconds).
const SLOW_REQUEST_WARN_THRESHOLD: Duration = Duration::from_secs(30);

use super::dispatch::{DispatchKey, IDENTITY_CID, IDENTITY_TOKEN_CID, SERVICE_OPEN_CID_TAG};
use super::state::{
    BqlState, BsrchState, BulkDataState, FieldInfoState, GenericState, HistDataState,
    HistDataStreamState, IntradayBarState, IntradayBarStreamState, IntradayTickState,
    IntradayTickStreamState, RefDataState,
};
use super::{
    attach_auth_context, build_session_options, EngineConfig, PlannedRequestShape, PreparedRequest,
    RequestParams, SlabKey, WorkerHealth, SESSION_STARTUP_TIMEOUT_MS,
};

fn iter_named_request_parameters(
    params: &RequestParams,
) -> impl Iterator<Item = (&str, &str)> + '_ {
    params
        .elements
        .iter()
        .flat_map(|pairs| pairs.iter())
        .chain(params.options.iter().flat_map(|pairs| pairs.iter()))
        .map(|(name, value)| (name.as_str(), value.as_str()))
}

fn apply_named_request_parameter(
    request: &mut xbbg_core::Request,
    name: &str,
    value: &str,
) -> Result<(), BlpError> {
    if name.contains('.') {
        if let Ok(int_val) = value.parse::<i32>() {
            request.set_nested_int(name, int_val)?;
        } else {
            request.set_nested_str(name, value)?;
        }
    } else if request.set_str(name, value).is_err() {
        request.append_str(name, value)?;
    }

    Ok(())
}

fn apply_excel_grid_request_parameters(
    request: &mut xbbg_core::Request,
    params: &RequestParams,
) -> Result<(), BlpError> {
    if let Some(domain) = params
        .elements
        .iter()
        .flat_map(|pairs| pairs.iter())
        .rev()
        .find(|(name, _)| name.eq_ignore_ascii_case("Domain"))
        .map(|(_, value)| value.as_str())
    {
        request.set_str("Domain", domain)?;
    }

    let Some(overrides) = params
        .overrides
        .as_ref()
        .filter(|values| !values.is_empty())
    else {
        return Ok(());
    };

    let overrides_ptr = request.get_or_create_element("Overrides")?;
    for (name, value) in overrides {
        if name.is_empty() {
            continue;
        }
        // SAFETY: overrides_ptr is a valid element obtained from get_or_create_element;
        // entry_ptr is valid from append_element and belongs to this request.
        let entry_ptr = unsafe { request.append_element(overrides_ptr)? };
        unsafe { request.set_element_string(entry_ptr, "name", name)? };
        unsafe { request.set_element_string(entry_ptr, "value", value)? };
    }

    Ok(())
}

/// Unified request state combining all request types.
#[allow(clippy::large_enum_variant)]
pub enum UnifiedRequestState {
    // Bulk request types (from Lane B)
    RefData(RefDataState),
    HistData(HistDataState),
    BulkData(BulkDataState),
    HistDataStream(HistDataStreamState),
    Generic(GenericState),
    Bql(BqlState),
    Bsrch(BsrchState),
    FieldInfo(FieldInfoState),
    // Intraday request types (from Lane C)
    IntradayBar(IntradayBarState),
    IntradayTick(IntradayTickState),
    IntradayBarStream(IntradayBarStreamState),
    IntradayTickStream(IntradayTickStreamState),
}

impl UnifiedRequestState {
    /// Process a PARTIAL_RESPONSE message (append to builders).
    pub fn on_partial(&mut self, msg: &xbbg_core::Message) {
        match self {
            // Bulk types
            UnifiedRequestState::RefData(s) => s.on_partial(msg),
            UnifiedRequestState::HistData(s) => s.on_partial(msg),
            UnifiedRequestState::BulkData(s) => s.on_partial(msg),
            UnifiedRequestState::HistDataStream(s) => s.on_partial(msg),
            UnifiedRequestState::Generic(s) => s.on_partial(msg),
            UnifiedRequestState::Bql(s) => s.on_partial(msg),
            UnifiedRequestState::Bsrch(s) => s.on_partial(msg),
            UnifiedRequestState::FieldInfo(s) => s.on_partial(msg),
            // Intraday types
            UnifiedRequestState::IntradayBar(s) => s.on_partial(msg),
            UnifiedRequestState::IntradayTick(s) => s.on_partial(msg),
            UnifiedRequestState::IntradayBarStream(s) => s.on_partial(msg),
            UnifiedRequestState::IntradayTickStream(s) => s.on_partial(msg),
        }
    }

    /// Process the final RESPONSE message, build the result, and send reply.
    pub fn finish_and_reply(self, msg: &xbbg_core::Message) {
        match self {
            // Bulk types
            UnifiedRequestState::RefData(s) => s.finish(msg),
            UnifiedRequestState::HistData(s) => s.finish(msg),
            UnifiedRequestState::BulkData(s) => s.finish(msg),
            UnifiedRequestState::HistDataStream(s) => s.finish(msg),
            UnifiedRequestState::Generic(s) => s.finish(msg),
            UnifiedRequestState::Bql(s) => s.finish(msg),
            UnifiedRequestState::Bsrch(s) => s.finish(msg),
            UnifiedRequestState::FieldInfo(s) => s.finish(msg),
            // Intraday types
            UnifiedRequestState::IntradayBar(s) => s.finish(msg),
            UnifiedRequestState::IntradayTick(s) => s.finish(msg),
            UnifiedRequestState::IntradayBarStream(s) => s.finish(msg),
            UnifiedRequestState::IntradayTickStream(s) => s.finish(msg),
        }
    }

    /// Handle a request failure/error.
    pub fn fail(self, error: BlpError) {
        match self {
            // Bulk types
            UnifiedRequestState::RefData(s) => {
                let _ = s.reply.send(Err(error));
            }
            UnifiedRequestState::HistData(s) => {
                let _ = s.reply.send(Err(error));
            }
            UnifiedRequestState::BulkData(s) => {
                let _ = s.reply.send(Err(error));
            }
            UnifiedRequestState::HistDataStream(s) => s.fail(error),
            UnifiedRequestState::Generic(s) => {
                let _ = s.reply.send(Err(error));
            }
            UnifiedRequestState::Bql(s) => {
                let _ = s.reply.send(Err(error));
            }
            UnifiedRequestState::Bsrch(s) => {
                let _ = s.reply.send(Err(error));
            }
            UnifiedRequestState::FieldInfo(s) => {
                let _ = s.reply.send(Err(error));
            }
            // Intraday types
            UnifiedRequestState::IntradayBar(s) => {
                let _ = s.reply.send(Err(error));
            }
            UnifiedRequestState::IntradayTick(s) => {
                let _ = s.reply.send(Err(error));
            }
            UnifiedRequestState::IntradayBarStream(s) => s.fail(error),
            UnifiedRequestState::IntradayTickStream(s) => s.fail(error),
        }
    }
}

fn send_stream_error(stream: mpsc::Sender<Result<RecordBatch, BlpError>>, error: BlpError) {
    let _ = stream.try_send(Err(error));
}

fn correlation_invariant_failure(
    expected: DispatchKey,
    actual: &CorrelationId,
    slab_key: usize,
) -> Option<String> {
    match DispatchKey::from_correlation_id(actual) {
        None => Some(format!(
            "Bloomberg returned non-dispatch correlation ID for request: {actual:?}"
        )),
        Some(actual_dispatch_key) if actual_dispatch_key != expected => Some(format!(
            "Bloomberg returned unexpected dispatch correlation ID {actual:?} for slab key {slab_key}"
        )),
        Some(_) => None,
    }
}

/// Identifies one in-flight request on a specific worker: slab slot plus the
/// generation encoded into its correlation ID. Required for cancellation.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RequestTicket {
    pub(crate) key: SlabKey,
    pub(crate) generation: u32,
}

/// Mutable state for one in-flight request.
struct RequestStateCell {
    /// Generation encoded into this request's correlation ID.
    generation: u32,
    /// Request decoder state. `None` means the request was completed, failed,
    /// timed out, or cancelled; late partials that already cloned this cell
    /// become no-ops.
    state: Mutex<Option<UnifiedRequestState>>,
    /// Partials that passed the slab generation check but have not completed
    /// their per-request decode yet. Final/error consumers wait for this to
    /// reach zero so they consume state after already-dispatched partials.
    active_partials: Mutex<usize>,
    active_partials_cv: Condvar,
}

impl RequestStateCell {
    fn new(generation: u32, state: UnifiedRequestState) -> Self {
        Self {
            generation,
            state: Mutex::new(Some(state)),
            active_partials: Mutex::new(0),
            active_partials_cv: Condvar::new(),
        }
    }

    fn begin_partial(self: &Arc<Self>) -> RequestPartialGuard {
        *self.active_partials.lock() += 1;
        RequestPartialGuard {
            cell: Arc::clone(self),
        }
    }

    fn wait_for_partials(&self) {
        let mut active = self.active_partials.lock();
        while *active != 0 {
            self.active_partials_cv.wait(&mut active);
        }
    }

    fn on_partial(&self, msg: &xbbg_core::Message<'_>) -> bool {
        let mut state = self.state.lock();
        let Some(state) = state.as_mut() else {
            return false;
        };
        state.on_partial(msg);
        true
    }

    fn finish_and_reply(&self, msg: &xbbg_core::Message<'_>) -> bool {
        self.wait_for_partials();
        let Some(state) = self.state.lock().take() else {
            return false;
        };
        state.finish_and_reply(msg);
        true
    }

    fn fail(&self, error: BlpError) -> bool {
        self.wait_for_partials();
        let Some(state) = self.state.lock().take() else {
            return false;
        };
        state.fail(error);
        true
    }

    fn discard(&self) -> bool {
        self.wait_for_partials();
        self.state.lock().take().is_some()
    }
}

struct RequestPartialGuard {
    cell: Arc<RequestStateCell>,
}

impl RequestPartialGuard {
    fn on_partial(&self, msg: &xbbg_core::Message<'_>) -> bool {
        self.cell.on_partial(msg)
    }
}

impl Drop for RequestPartialGuard {
    fn drop(&mut self) {
        let mut active = self.cell.active_partials.lock();
        debug_assert!(*active > 0);
        *active = active.saturating_sub(1);
        if *active == 0 {
            self.cell.active_partials_cv.notify_all();
        }
    }
}

/// One in-flight request slot.
struct RequestSlot {
    /// When the request was registered (just before `sendRequest`).
    sent_at: Instant,
    /// Whether the slow-request warning has fired for this slot.
    warned: bool,
    cell: Arc<RequestStateCell>,
}

/// A pending async `open_service` call plus everyone awaiting its outcome.
struct PendingServiceOpen {
    cid: i64,
    waiters: Vec<oneshot::Sender<Result<(), BlpError>>>,
}

/// Authorization state of this worker's on-demand session identity
/// (`generateAuthorizedIdentityAsync` tagged with [`IDENTITY_CID`]; used
/// when the engine has an [`AuthConfig`] — the SDK then owns the authorized
/// identity and `authorized_identity(IDENTITY_CID)` hands out fresh
/// refcounted handles per query, so no identity is ever stored or crosses
/// threads).
///
/// Terminal outcomes never wedge: `Failed` is retryable (the next call
/// re-issues the SDK call), and a waiter timeout fails the pending attempt
/// rather than leaving zombie waiters accumulating (the lesson from the
/// service-open latch).
#[derive(Default)]
enum IdentityAuthState {
    /// No authorization attempted yet.
    #[default]
    NotRequested,
    /// `generateAuthorizedIdentityAsync` issued; waiters await the
    /// AUTHORIZATION_STATUS outcome.
    Pending(Vec<oneshot::Sender<Result<(), BlpError>>>),
    /// AuthorizationSuccess received; `authorized_identity(IDENTITY_CID)` is
    /// ready to hand out fresh handles.
    Ready,
    /// Authorization failed, timed out, or was revoked; retry allowed.
    Failed(String),
}

/// Session startup outcome latch, resolved exactly once by the first
/// startup-relevant SESSION_STATUS message.
#[derive(Default)]
struct StartupLatch {
    resolved: bool,
    result: Option<Result<(), BlpError>>,
}

/// State shared between submitter threads and the SDK dispatcher callback.
pub(super) struct WorkerShared {
    id: usize,
    /// Slab for O(1) correlation dispatch. Slots carry the generation tag
    /// encoded into the request's correlation ID so dispatch can reject
    /// stale messages after a slot is recycled.
    requests: Mutex<Slab<RequestSlot>>,
    /// Wrapping generation counter encoded into request dispatch CIDs.
    next_generation: AtomicU32,
    /// Services opened on this worker's session.
    open_services: RwLock<HashSet<String>>,
    /// Pending async `open_service` calls keyed by service name; resolved by
    /// `handle_service_status` matching on the open's correlation ID.
    pending_service_opens: Mutex<HashMap<String, PendingServiceOpen>>,
    /// Counter for generating unique service-open CIDs.
    next_service_open_id: AtomicI64,
    startup: Mutex<StartupLatch>,
    startup_cv: Condvar,
    /// On-demand session-identity latch (auth-configured engines); see
    /// [`IdentityAuthState`].
    identity: Mutex<IdentityAuthState>,
    /// In-flight `generateToken` waiter (classic per-call authorization on
    /// engines without auth config; fulfilled from the dispatcher thread,
    /// received with a timeout on the classic flow's blocking thread).
    pending_token: Mutex<Option<std::sync::mpsc::SyncSender<Result<String, BlpError>>>>,
    /// In-flight classic `AuthorizationRequest` waiter.
    pending_identity_auth: Mutex<Option<std::sync::mpsc::SyncSender<Result<(), BlpError>>>>,
    health: Arc<AtomicU8>,
    /// Set before an intentional stop so terminal SDK status events are logged
    /// as normal teardown instead of unexpected worker death.
    shutting_down: AtomicBool,
    /// Dispatches take a shared guard; shutdown takes the exclusive guard so
    /// no SDK request can be sent after the lifecycle gate becomes terminal.
    lifecycle: RwLock<()>,
}

struct PendingServiceWaiter<'a> {
    shared: &'a WorkerShared,
    service: &'a str,
    attempt_cid: i64,
    receiver: Option<oneshot::Receiver<Result<(), BlpError>>>,
}

impl<'a> PendingServiceWaiter<'a> {
    fn new(
        shared: &'a WorkerShared,
        service: &'a str,
        attempt_cid: i64,
        receiver: oneshot::Receiver<Result<(), BlpError>>,
    ) -> Self {
        Self {
            shared,
            service,
            attempt_cid,
            receiver: Some(receiver),
        }
    }

    fn receiver(&mut self) -> &mut oneshot::Receiver<Result<(), BlpError>> {
        self.receiver.as_mut().expect("receiver is present")
    }
}

impl Drop for PendingServiceWaiter<'_> {
    fn drop(&mut self) {
        self.receiver.take();
        self.shared
            .prune_pending_service_waiters(self.service, self.attempt_cid);
    }
}

struct PendingIdentityWaiter<'a> {
    shared: &'a WorkerShared,
    receiver: Option<oneshot::Receiver<Result<(), BlpError>>>,
}

impl<'a> PendingIdentityWaiter<'a> {
    fn new(shared: &'a WorkerShared, receiver: oneshot::Receiver<Result<(), BlpError>>) -> Self {
        Self {
            shared,
            receiver: Some(receiver),
        }
    }

    fn receiver(&mut self) -> &mut oneshot::Receiver<Result<(), BlpError>> {
        self.receiver.as_mut().expect("receiver is present")
    }
}

impl Drop for PendingIdentityWaiter<'_> {
    fn drop(&mut self) {
        self.receiver.take();
        self.shared.prune_pending_identity_waiters();
    }
}

impl WorkerShared {
    fn new(id: usize, health: Arc<AtomicU8>) -> Self {
        Self {
            id,
            requests: Mutex::new(Slab::new()),
            next_generation: AtomicU32::new(0),
            open_services: RwLock::new(HashSet::new()),
            pending_service_opens: Mutex::new(HashMap::new()),
            next_service_open_id: AtomicI64::new(0),
            startup: Mutex::new(StartupLatch::default()),
            startup_cv: Condvar::new(),
            identity: Mutex::new(IdentityAuthState::default()),
            pending_token: Mutex::new(None),
            pending_identity_auth: Mutex::new(None),
            health,
            shutting_down: AtomicBool::new(false),
            lifecycle: RwLock::new(()),
        }
    }

    /// Next wrapping generation tag for request dispatch CIDs.
    fn next_generation(&self) -> u32 {
        self.next_generation
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1)
    }

    fn insert_slot(&self, generation: u32, state: UnifiedRequestState) -> SlabKey {
        self.requests.lock().insert(RequestSlot {
            sent_at: Instant::now(),
            warned: false,
            cell: Arc::new(RequestStateCell::new(generation, state)),
        })
    }

    /// Remove and return the slot addressed by `dispatch_key` iff its
    /// generation matches; stale keys (recycled slots) are dropped.
    fn take_slot(&self, dispatch_key: DispatchKey) -> Option<RequestSlot> {
        let key = dispatch_key.to_slab_key();
        let mut requests = self.requests.lock();
        match requests.get(key) {
            Some(slot) if slot.cell.generation == dispatch_key.generation() => {
                Some(requests.remove(key))
            }
            Some(_) => {
                xbbg_log::debug!(
                    worker_id = self.id,
                    key = key,
                    "stale message for recycled slot; dropped"
                );
                None
            }
            None => None,
        }
    }

    fn resolve_startup(&self, result: Result<(), BlpError>) {
        let mut latch = self.startup.lock();
        if !latch.resolved {
            latch.resolved = true;
            latch.result = Some(result);
            self.startup_cv.notify_all();
        }
    }

    /// Block until the first startup-relevant status arrives (or `timeout`).
    fn wait_startup(&self, timeout: Duration) -> Result<(), BlpError> {
        let deadline = Instant::now() + timeout;
        let mut latch = self.startup.lock();
        while latch.result.is_none() {
            if self.startup_cv.wait_until(&mut latch, deadline).timed_out() {
                return Err(BlpError::Timeout);
            }
        }
        latch.result.take().expect("checked above")
    }

    /// Mark expired slow requests and return hard-timeout candidates.
    pub(super) fn scan_timeouts(&self, hard_timeout: Option<Duration>) -> Vec<RequestTicket> {
        let now = Instant::now();
        let mut expired = Vec::new();
        let mut requests = self.requests.lock();
        for (key, slot) in requests.iter_mut() {
            let elapsed = now.duration_since(slot.sent_at);
            if elapsed > SLOW_REQUEST_WARN_THRESHOLD && !slot.warned {
                slot.warned = true;
                xbbg_log::warn!(
                    worker_id = self.id,
                    request_key = key,
                    elapsed_secs = elapsed.as_secs(),
                    "request outstanding longer than expected; still waiting on Bloomberg"
                );
            }
            if let Some(hard) = hard_timeout {
                if elapsed >= hard {
                    expired.push(RequestTicket {
                        key,
                        generation: slot.cell.generation,
                    });
                }
            }
        }
        expired
    }

    fn drain_in_flight(&self, reason: &str) {
        let drained: Vec<RequestSlot> = {
            let mut requests = self.requests.lock();
            requests.drain().collect()
        };
        if drained.is_empty() {
            return;
        }
        let count = drained.len();
        for slot in drained {
            slot.cell.fail(BlpError::Internal {
                detail: format!("{} (worker={})", reason, self.id),
            });
        }
        xbbg_log::error!(
            worker_id = self.id,
            drained = count,
            reason = reason,
            "drained in-flight requests"
        );
    }

    fn prune_pending_service_waiters(&self, service: &str, attempt_cid: i64) {
        let mut pending = self.pending_service_opens.lock();
        let remove_attempt = if let Some(open) = pending.get_mut(service) {
            if open.cid != attempt_cid {
                return;
            }
            open.waiters.retain(|waiter| !waiter.is_closed());
            open.waiters.is_empty()
        } else {
            false
        };
        if remove_attempt {
            pending.remove(service);
        }
    }

    fn prune_pending_identity_waiters(&self) {
        if let IdentityAuthState::Pending(waiters) = &mut *self.identity.lock() {
            waiters.retain(|waiter| !waiter.is_closed());
        }
    }

    fn fail_pending_service_opens(&self, reason: &str) {
        let drained: Vec<(String, PendingServiceOpen)> = {
            let mut pending = self.pending_service_opens.lock();
            pending.drain().collect()
        };
        for (service, open) in drained {
            for waiter in open.waiters {
                let _ = waiter.send(Err(BlpError::OpenService {
                    service: service.clone(),
                    source: None,
                    label: Some(reason.to_string()),
                }));
            }
        }
    }

    /// Resolve the identity latch to a terminal state and notify waiters.
    /// `Ok(())` transitions to `Ready`; `Err` to retryable `Failed`.
    fn resolve_identity(&self, outcome: Result<(), BlpError>) {
        let detail = outcome.as_ref().err().map(ToString::to_string);
        let waiters = {
            let mut state = self.identity.lock();
            let next = match &detail {
                None => IdentityAuthState::Ready,
                Some(e) => IdentityAuthState::Failed(e.clone()),
            };
            match std::mem::replace(&mut *state, next) {
                IdentityAuthState::Pending(waiters) => waiters,
                _ => Vec::new(),
            }
        };
        for waiter in waiters {
            // BlpError is not Clone; rebuild per waiter from the detail string.
            let _ = waiter.send(match &detail {
                None => Ok(()),
                Some(e) => Err(BlpError::Internal { detail: e.clone() }),
            });
        }
    }

    /// Fail any in-flight identity authorization (session teardown paths) —
    /// both the session-identity latch and the classic per-call waiters.
    fn fail_pending_identity(&self, reason: &str) {
        let is_pending = matches!(&*self.identity.lock(), IdentityAuthState::Pending(_));
        if is_pending {
            self.resolve_identity(Err(BlpError::Internal {
                detail: format!("identity authorization aborted: {reason}"),
            }));
        }
        if let Some(tx) = self.pending_token.lock().take() {
            let _ = tx.send(Err(BlpError::Internal {
                detail: format!("identity token generation aborted: {reason}"),
            }));
        }
        if let Some(tx) = self.pending_identity_auth.lock().take() {
            let _ = tx.send(Err(BlpError::Internal {
                detail: format!("identity authorization aborted: {reason}"),
            }));
        }
    }

    /// Route an authorization outcome for [`IDENTITY_CID`] to whichever flow
    /// is in flight: the session-identity latch (auth-configured engines,
    /// `generateAuthorizedIdentityAsync`) or the classic per-call waiter
    /// (`sendAuthorizationRequest`). Only one flow ever runs per worker —
    /// selection is fixed by `EngineConfig.auth` — so fulfilling both is a
    /// no-op for the idle one.
    fn resolve_identity_outcome(&self, outcome: Result<(), BlpError>) {
        let detail = outcome.as_ref().err().map(ToString::to_string);
        if let Some(tx) = self.pending_identity_auth.lock().take() {
            let _ = tx.send(match &detail {
                None => Ok(()),
                Some(e) => Err(BlpError::Internal { detail: e.clone() }),
            });
        }
        self.resolve_identity(outcome);
    }

    /// TOKEN_STATUS handler for the classic authorization flow's
    /// `generateToken` call ([`IDENTITY_TOKEN_CID`]).
    fn handle_token_status(&self, msg: &xbbg_core::Message<'_>) {
        let ours = msg.correlation_ids().any(|cid| {
            matches!(
                cid,
                CorrelationId::Int(value) if value == IDENTITY_TOKEN_CID
            )
        });
        if !ours {
            return;
        }
        match msg.type_str() {
            "TokenGenerationSuccess" => {
                let token = msg
                    .elements()
                    .get_by_str("token")
                    .and_then(|e| e.get_str(0))
                    .map(str::to_string);
                if let Some(tx) = self.pending_token.lock().take() {
                    let _ = tx.send(token.ok_or_else(|| BlpError::Internal {
                        detail: "TokenGenerationSuccess carried no token element".to_string(),
                    }));
                }
            }
            "TokenGenerationFailure" => {
                let reason = extract_reason_description(msg)
                    .unwrap_or_else(|| "TokenGenerationFailure".to_string());
                xbbg_log::warn!(
                    worker_id = self.id,
                    reason = reason.as_str(),
                    "identity token generation failed"
                );
                if let Some(tx) = self.pending_token.lock().take() {
                    let _ = tx.send(Err(BlpError::Internal {
                        detail: format!("identity token generation failed: {reason}"),
                    }));
                }
            }
            other => {
                xbbg_log::debug!(
                    worker_id = self.id,
                    message_type = other,
                    "unhandled token status message"
                );
            }
        }
    }

    /// Authorization outcome for [`IDENTITY_CID`], regardless of which event
    /// type carried it (`AUTHORIZATION_STATUS` for the session-identity flow;
    /// classic `sendAuthorizationRequest` replies arrive as RESPONSE /
    /// PARTIAL_RESPONSE). Session-identity authorization messages from
    /// `setSessionIdentityOptions` arrive as SESSION_STATUS and are handled
    /// in `handle_session_status`.
    fn handle_identity_authorization_message(&self, msg: &xbbg_core::Message<'_>) {
        match msg.type_str() {
            "AuthorizationSuccess" => {
                xbbg_log::info!(worker_id = self.id, "identity authorized");
                self.resolve_identity_outcome(Ok(()));
            }
            "AuthorizationFailure" => {
                let reason = extract_reason_description(msg);
                xbbg_log::warn!(
                    worker_id = self.id,
                    reason = %reason.as_deref().unwrap_or(""),
                    "identity authorization failed"
                );
                self.resolve_identity_outcome(Err(BlpError::Internal {
                    detail: format!(
                        "identity authorization failed: {}",
                        reason.unwrap_or_else(|| "AuthorizationFailure".to_string())
                    ),
                }));
            }
            "AuthorizationRevoked" => {
                let reason = extract_reason_description(msg);
                xbbg_log::warn!(
                    worker_id = self.id,
                    reason = %reason.as_deref().unwrap_or(""),
                    "identity authorization revoked"
                );
                // Mark Failed so the next identity op re-authorizes instead of
                // serving entitlement answers from a revoked identity.
                *self.identity.lock() = IdentityAuthState::Failed(format!(
                    "identity authorization revoked: {}",
                    reason.unwrap_or_default()
                ));
            }
            other => {
                xbbg_log::debug!(
                    worker_id = self.id,
                    message_type = other,
                    "unhandled authorization message"
                );
            }
        }
    }

    /// True when `msg` is tagged with the identity authorization CID.
    fn is_identity_auth_message(msg: &xbbg_core::Message<'_>) -> bool {
        msg.correlation_ids().any(|cid| {
            matches!(
                cid,
                CorrelationId::Int(value) if value == IDENTITY_CID
            )
        })
    }

    /// AUTHORIZATION_STATUS handler: identity outcomes for [`IDENTITY_CID`].
    fn handle_authorization_status(&self, msg: &xbbg_core::Message<'_>) {
        if Self::is_identity_auth_message(msg) {
            self.handle_identity_authorization_message(msg);
        }
    }

    /// SDK dispatcher entry point: route every message of `ev`.
    pub(super) fn dispatch_event(&self, ev: xbbg_core::Event) {
        let et = ev.event_type();

        // CRITICAL: iterate ALL messages, never break early
        let mut messages = ev.messages();
        while let Some(msg) = messages.next() {
            match et {
                EventType::PartialResponse => {
                    self.handle_partial_response(&msg);
                }
                EventType::Response => {
                    self.handle_response(&msg);
                }
                EventType::RequestStatus => {
                    self.handle_request_status(&msg);
                }
                EventType::SessionStatus => {
                    self.handle_session_status(&msg);
                }
                EventType::ServiceStatus => {
                    self.handle_service_status(&msg);
                }
                EventType::AuthorizationStatus => {
                    self.handle_authorization_status(&msg);
                }
                EventType::TokenStatus => {
                    self.handle_token_status(&msg);
                }
                _ => {}
            }
        }
    }

    fn handle_partial_response(&self, msg: &xbbg_core::Message<'_>) {
        // Classic-flow authorization replies arrive as ordinary responses
        // tagged with IDENTITY_CID (rejected by DispatchKey, so they can
        // never alias a request slot).
        if Self::is_identity_auth_message(msg) {
            self.handle_identity_authorization_message(msg);
            return;
        }
        for correlation_id in msg.correlation_ids() {
            let Some(dispatch_key) = DispatchKey::from_correlation_id(&correlation_id) else {
                continue;
            };
            let key = dispatch_key.to_slab_key();
            let partial = {
                let requests = self.requests.lock();
                match requests.get(key) {
                    Some(slot) if slot.cell.generation == dispatch_key.generation() => {
                        Some(slot.cell.begin_partial())
                    }
                    Some(_) => {
                        xbbg_log::debug!(
                            worker_id = self.id,
                            key = key,
                            "stale partial response for recycled slot; dropped"
                        );
                        None
                    }
                    None => None,
                }
            };
            if let Some(partial) = partial {
                if partial.on_partial(msg) {
                    xbbg_log::trace!(worker_id = self.id, key = key, "partial response");
                }
            }
        }
    }

    fn handle_response(&self, msg: &xbbg_core::Message<'_>) {
        if Self::is_identity_auth_message(msg) {
            self.handle_identity_authorization_message(msg);
            return;
        }
        for correlation_id in msg.correlation_ids() {
            let Some(dispatch_key) = DispatchKey::from_correlation_id(&correlation_id) else {
                continue;
            };
            if let Some(slot) = self.take_slot(dispatch_key) {
                let key = dispatch_key.to_slab_key();
                let rtt_ms = slot.sent_at.elapsed().as_micros() as f64 / 1000.0;
                xbbg_log::debug!(
                    worker_id = self.id,
                    rtt_ms = rtt_ms,
                    key = key,
                    "bloomberg_roundtrip"
                );
                // Build the final batch outside the slab lock.
                if slot.cell.finish_and_reply(msg) {
                    xbbg_log::debug!(worker_id = self.id, key = key, "response completed");
                }
            }
        }
    }

    fn handle_request_status(&self, msg: &xbbg_core::Message<'_>) {
        let msg_type = msg.type_str();
        if msg_type != "RequestFailure" {
            return;
        }
        // A failed classic AuthorizationRequest resolves the identity waiter.
        if Self::is_identity_auth_message(msg) {
            let reason =
                extract_reason_description(msg).unwrap_or_else(|| "RequestFailure".to_string());
            self.resolve_identity_outcome(Err(BlpError::Internal {
                detail: format!("identity authorization request failed: {reason}"),
            }));
            return;
        }
        for correlation_id in msg.correlation_ids() {
            let Some(dispatch_key) = DispatchKey::from_correlation_id(&correlation_id) else {
                continue;
            };
            let reason = extract_reason_description(msg);
            xbbg_log::error!(
                worker_id = self.id,
                key = dispatch_key.to_slab_key(),
                reason = %reason.as_deref().unwrap_or(""),
                "request failed"
            );
            if let Some(slot) = self.take_slot(dispatch_key) {
                slot.cell.fail(BlpError::Internal {
                    detail: reason.unwrap_or_else(|| "RequestFailure".to_string()),
                });
            }
        }
    }

    fn handle_session_status(&self, msg: &xbbg_core::Message<'_>) {
        let msg_type = msg.type_str();
        match msg_type {
            "SessionStarted" => {
                if !self.shutting_down.load(Ordering::Acquire) {
                    self.health.store(0, Ordering::Release);
                }
                self.resolve_startup(Ok(()));
                xbbg_log::info!(worker_id = self.id, "session started");
            }
            "SessionStartupFailure" => {
                let reason = extract_reason_description(msg);
                xbbg_log::error!(
                    worker_id = self.id,
                    reason = %reason.as_deref().unwrap_or(""),
                    "session startup failure"
                );
                self.health.store(2, Ordering::Release);
                self.resolve_startup(Err(session_start_error("session startup failure", reason)));
            }
            "SessionTerminated" => {
                // SDK has given up reconnecting. The session is dead. Drain
                // everything and mark the worker so the pool evicts it.
                let reason = extract_reason_description(msg);
                let shutdown_requested = self.shutting_down.swap(true, Ordering::AcqRel);
                self.resolve_startup(Err(session_start_error(
                    "session terminated during startup",
                    reason.clone(),
                )));
                self.drain_in_flight(reason.as_deref().unwrap_or("Bloomberg session terminated"));
                self.fail_pending_service_opens("Bloomberg session terminated");
                self.fail_pending_identity("Bloomberg session terminated");
                self.health.store(2, Ordering::Release);
                if shutdown_requested {
                    xbbg_log::info!(
                        worker_id = self.id,
                        reason = %reason.as_deref().unwrap_or(""),
                        "SessionTerminated during requested shutdown"
                    );
                } else {
                    xbbg_log::error!(
                        worker_id = self.id,
                        reason = %reason.as_deref().unwrap_or(""),
                        "SessionTerminated — worker is dead"
                    );
                }
            }
            "AuthorizationFailure" => {
                let reason = extract_reason_description(msg);
                self.resolve_startup(Err(session_start_error(
                    "session identity authorization failed",
                    reason,
                )));
            }
            "AuthorizationRevoked" => {
                // Session identity was revoked mid-session. Authorized requests
                // will now fail. Drain + mark Dead so the pool evicts this
                // worker.
                let reason = extract_reason_description(msg);
                self.shutting_down.store(true, Ordering::Release);
                self.resolve_startup(Err(session_start_error(
                    "session identity authorization revoked",
                    reason.clone(),
                )));
                self.drain_in_flight(
                    reason
                        .as_deref()
                        .unwrap_or("Bloomberg session identity revoked"),
                );
                self.fail_pending_service_opens("Bloomberg session identity revoked");
                self.fail_pending_identity("Bloomberg session identity revoked");
                self.health.store(2, Ordering::Release);
                xbbg_log::error!(
                    worker_id = self.id,
                    reason = %reason.as_deref().unwrap_or(""),
                    "AuthorizationRevoked — identity gone; worker is dead"
                );
            }
            "SessionConnectionDown" => {
                // Transient network drop. Unlike subscriptions (which the SDK
                // auto-recovers via SubscriptionStreamsActivated/Deactivated),
                // requests are transactional: any response mid-transit when TCP
                // dropped is lost. Fail in-flight requests immediately so the
                // caller can retry on a healthy worker or back off. The session
                // is still alive and the SDK will auto-reconnect, so we go
                // Degraded (not Dead) — the worker resumes full service on the
                // subsequent SessionConnectionUp.
                let reason = extract_reason_description(msg);
                let shutdown_requested = self.shutting_down.load(Ordering::Acquire);
                self.drain_in_flight(
                    reason
                        .as_deref()
                        .unwrap_or("Bloomberg session connection lost (transient)"),
                );
                if !shutdown_requested {
                    self.health.store(1, Ordering::Release);
                }
                if shutdown_requested {
                    xbbg_log::info!(
                        worker_id = self.id,
                        reason = %reason.as_deref().unwrap_or(""),
                        "SessionConnectionDown during requested shutdown"
                    );
                } else {
                    xbbg_log::warn!(
                        worker_id = self.id,
                        reason = %reason.as_deref().unwrap_or(""),
                        "SessionConnectionDown — failing in-flight requests; SDK will auto-reconnect"
                    );
                }
            }
            "SessionConnectionUp" => {
                let reason = extract_reason_description(msg);
                if self.shutting_down.load(Ordering::Acquire) {
                    xbbg_log::info!(
                        worker_id = self.id,
                        reason = %reason.as_deref().unwrap_or(""),
                        "SessionConnectionUp during shutdown"
                    );
                } else {
                    self.health.store(0, Ordering::Release);
                    xbbg_log::info!(
                        worker_id = self.id,
                        reason = %reason.as_deref().unwrap_or(""),
                        "SessionConnectionUp — worker healthy"
                    );
                }
            }
            _ => {
                xbbg_log::debug!(worker_id = self.id, msg_type = msg_type, "session status");
            }
        }
    }

    fn handle_service_status(&self, msg: &xbbg_core::Message<'_>) {
        let msg_type = msg.type_str();

        // If this ServiceOpened/ServiceOpenFailure is a reply to one of our
        // async `open_service_async` calls, resolve the matching pending open
        // so every waiting `ensure_service` unblocks.
        if matches!(msg_type, "ServiceOpened" | "ServiceOpenFailure") {
            if let Some(CorrelationId::Int(cid_int)) = msg.correlation_id(0) {
                let entry = {
                    let mut pending = self.pending_service_opens.lock();
                    let service = pending
                        .iter()
                        .find_map(|(name, open)| (open.cid == cid_int).then(|| name.clone()));
                    service.and_then(|name| pending.remove(&name).map(|open| (name, open)))
                };
                if let Some((service, open)) = entry {
                    if msg_type == "ServiceOpened" {
                        self.open_services.write().insert(service.clone());
                        xbbg_log::debug!(worker_id = self.id, service = %service, "service opened");
                        for waiter in open.waiters {
                            let _ = waiter.send(Ok(()));
                        }
                    } else {
                        let reason = extract_reason_description(msg);
                        xbbg_log::warn!(
                            worker_id = self.id,
                            service = %service,
                            reason = %reason.as_deref().unwrap_or(""),
                            "service open failed"
                        );
                        for waiter in open.waiters {
                            let _ = waiter.send(Err(BlpError::OpenService {
                                service: service.clone(),
                                source: None,
                                label: reason.clone(),
                            }));
                        }
                    }
                    return;
                }
            }
        }

        xbbg_log::debug!(worker_id = self.id, msg_type = msg_type, "service status");
    }
}

fn session_start_error(context: &str, reason: Option<String>) -> BlpError {
    BlpError::SessionStart {
        source: None,
        label: Some(match reason {
            Some(reason) => format!("{context}: {reason}"),
            None => context.to_string(),
        }),
    }
}

/// Append configuration guidance to classic-flow authorization failures so
/// the vendor reason (e.g. "User not in emrs userid=...") arrives with the
/// remedy attached.
fn hint_entitlement_requirements(err: BlpError) -> BlpError {
    match err {
        BlpError::Internal { detail } => BlpError::Internal {
            detail: format!(
                "{detail} (identity/entitlement checks require EMRS enrollment for \
                 Desktop API terminals, or SAPI/B-PIPE authorization via EngineConfig.auth)"
            ),
        },
        other => other,
    }
}

/// A request worker backed by an asynchronous Bloomberg session.
///
/// All methods take `&self`; submissions, cancellations, and SDK callback
/// dispatch may run concurrently from different threads.
pub(crate) struct AsyncRequestWorker {
    pub(crate) id: usize,
    session: AsyncSession,
    shared: Arc<WorkerShared>,
    config: Arc<EngineConfig>,
    /// Serializes classic per-call identity authorization: WorkerShared's
    /// pending-waiter slots hold exactly one in-flight sender each.
    identity_flow_lock: tokio::sync::Mutex<()>,
    /// Guards the actual SDK stop request independently from terminal events,
    /// which may mark the shared lifecycle closed before pool cleanup runs.
    stop_started: AtomicBool,
}

impl AsyncRequestWorker {
    /// Create a worker: build options, start the session (blocking), wait for
    /// the startup status, and pre-warm configured services.
    pub(crate) fn new(id: usize, config: Arc<EngineConfig>) -> Result<Self, BlpError> {
        let options = build_session_options(&config, false)?;
        let health = Arc::new(AtomicU8::new(0));
        let shared = Arc::new(WorkerShared::new(id, health));

        let handler_shared = Arc::clone(&shared);
        let session = AsyncSession::new(&options, move |event| {
            handler_shared.dispatch_event(event);
        })?;

        session
            .start()
            .map_err(|err| attach_auth_context(err, config.auth.as_ref()))?;
        shared
            .wait_startup(Duration::from_millis(u64::from(SESSION_STARTUP_TIMEOUT_MS)))
            .map_err(|err| attach_auth_context(err, config.auth.as_ref()))?;

        let worker = Self {
            id,
            session,
            shared,
            config,
            identity_flow_lock: tokio::sync::Mutex::new(()),
            stop_started: AtomicBool::new(false),
        };
        worker.warmup();
        xbbg_log::info!(worker_id = id, "AsyncRequestWorker started");
        Ok(worker)
    }

    /// Pre-warm commonly used services. Blocking; only called at creation.
    /// Synchronous opens do not stall event delivery on an async session —
    /// events keep flowing on the SDK dispatcher thread.
    fn warmup(&self) {
        for service_name in &self.config.warmup_services {
            match self.session.open_service(service_name) {
                Ok(()) => {
                    self.shared
                        .open_services
                        .write()
                        .insert(service_name.clone());
                }
                Err(e) => {
                    xbbg_log::warn!(
                        worker_id = self.id,
                        service = %service_name,
                        error = %e,
                        "failed to pre-warm service"
                    );
                }
            }
        }
        xbbg_log::info!(
            worker_id = self.id,
            services = ?self.shared.open_services.read().iter().collect::<Vec<_>>(),
            "worker pre-warmed"
        );
    }

    pub(crate) fn health(&self) -> WorkerHealth {
        match self.shared.health.load(Ordering::Acquire) {
            0 => WorkerHealth::Healthy,
            1 => WorkerHealth::Degraded,
            _ => WorkerHealth::Dead,
        }
    }

    /// Ensure a service is open, awaiting an async open if necessary.
    /// Concurrent opens of the same service coalesce onto one SDK call.
    async fn ensure_service(&self, name: &str) -> Result<(), BlpError> {
        if self.shared.open_services.read().contains(name) {
            return Ok(());
        }

        let (attempt_cid, rx) = {
            let _lifecycle = self.shared.lifecycle.read();
            if self.is_shutting_down() {
                return Err(Self::shutdown_error());
            }
            let mut pending = self.shared.pending_service_opens.lock();
            // Re-check under the pending lock: the dispatcher inserts into
            // open_services before resolving waiters.
            if self.shared.open_services.read().contains(name) {
                return Ok(());
            }
            let (tx, rx) = oneshot::channel();
            let attempt_cid = match pending.entry(name.to_string()) {
                Entry::Occupied(mut entry) => {
                    let open = entry.get_mut();
                    open.waiters.retain(|waiter| !waiter.is_closed());
                    open.waiters.push(tx);
                    open.cid
                }
                Entry::Vacant(entry) => {
                    let id = self
                        .shared
                        .next_service_open_id
                        .fetch_add(1, Ordering::Relaxed)
                        .wrapping_add(1);
                    let cid_int = SERVICE_OPEN_CID_TAG | (id & (SERVICE_OPEN_CID_TAG - 1));
                    let cid = CorrelationId::Int(cid_int);
                    // The enqueue-only FFI call is cheap; holding the lock
                    // across it closes the insert/resolve race.
                    self.session.open_service_async(name, &cid)?;
                    entry.insert(PendingServiceOpen {
                        cid: cid_int,
                        waiters: vec![tx],
                    });
                    cid_int
                }
            };
            (attempt_cid, rx)
        };
        let mut waiter = PendingServiceWaiter::new(&self.shared, name, attempt_cid, rx);

        match tokio::time::timeout(
            Duration::from_millis(SERVICE_OPEN_TIMEOUT_MS),
            waiter.receiver(),
        )
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(BlpError::Internal {
                detail: format!("service open for {name} dropped without resolution"),
            }),
            Err(_) => {
                let timed_out = {
                    let mut pending = self.shared.pending_service_opens.lock();
                    if pending
                        .get(name)
                        .is_some_and(|open| open.cid == attempt_cid)
                    {
                        pending.remove(name)
                    } else {
                        None
                    }
                };
                if let Some(open) = timed_out {
                    for waiter in open.waiters {
                        let _ = waiter.send(Err(BlpError::Timeout));
                    }
                }
                Err(BlpError::Timeout)
            }
        }
    }

    /// Ensure the SDK-owned session identity is authorized, lazily issuing
    /// `generateAuthorizedIdentityAsync` with the engine's configured auth on
    /// first use. Auth-configured engines only.
    ///
    /// Concurrent callers coalesce onto one in-flight authorization; a
    /// timeout fails the attempt (draining all waiters) so the next call
    /// re-issues rather than piling waiters onto a zombie attempt.
    async fn ensure_identity(&self, auth_config: &AuthConfig) -> Result<(), BlpError> {
        let rx = {
            let _lifecycle = self.shared.lifecycle.read();
            if self.is_shutting_down() {
                return Err(Self::shutdown_error());
            }
            let mut state = self.shared.identity.lock();
            match &mut *state {
                IdentityAuthState::Ready => return Ok(()),
                IdentityAuthState::Pending(waiters) => {
                    waiters.retain(|waiter| !waiter.is_closed());
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    rx
                }
                state @ (IdentityAuthState::NotRequested | IdentityAuthState::Failed(_)) => {
                    if let IdentityAuthState::Failed(prior) = state {
                        xbbg_log::debug!(
                            worker_id = self.id,
                            prior_failure = prior.as_str(),
                            "retrying identity authorization"
                        );
                    }
                    let auth_options = auth_config.build_auth_options()?;
                    // Enqueue-only FFI call; holding the lock across it closes
                    // the issue/resolve race with the dispatcher thread.
                    self.session.generate_authorized_identity_async(
                        &auth_options,
                        &CorrelationId::Int(IDENTITY_CID),
                    )?;
                    let (tx, rx) = oneshot::channel();
                    *state = IdentityAuthState::Pending(vec![tx]);
                    xbbg_log::debug!(
                        worker_id = self.id,
                        auth_method = auth_config.method_name(),
                        "identity authorization started"
                    );
                    rx
                }
            }
        };
        let mut waiter = PendingIdentityWaiter::new(&self.shared, rx);

        match tokio::time::timeout(
            Duration::from_millis(IDENTITY_AUTH_TIMEOUT_MS),
            waiter.receiver(),
        )
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(BlpError::Internal {
                detail: "identity authorization dropped without resolution".to_string(),
            }),
            Err(_) => {
                // Fail the whole attempt so later calls re-issue instead of
                // waiting on an authorization the SDK may never answer.
                self.shared
                    .fail_pending_identity("identity authorization timed out");
                Err(BlpError::Timeout)
            }
        }
    }

    /// Blocking receive for a classic-flow waiter, clearing the pending slot
    /// on timeout so a late dispatcher fulfillment becomes a no-op and the
    /// next call retries. Runs on the classic flow's dedicated blocking
    /// thread, never on a runtime thread.
    fn recv_classic_step<T>(
        &self,
        rx: &std::sync::mpsc::Receiver<Result<T, BlpError>>,
        pending: &Mutex<Option<std::sync::mpsc::SyncSender<Result<T, BlpError>>>>,
        step: &str,
    ) -> Result<T, BlpError> {
        match rx.recv_timeout(Duration::from_millis(IDENTITY_AUTH_TIMEOUT_MS)) {
            Ok(result) => result,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                pending.lock().take();
                Err(BlpError::Timeout)
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(BlpError::Internal {
                detail: format!("identity {step} dropped without resolution"),
            }),
        }
    }

    /// Classic per-call authorization for engines WITHOUT auth config:
    /// `generateToken` (OS logon) → `//blp/apiauth` `AuthorizationRequest` →
    /// authorized [`xbbg_core::Identity`], then `op` — all on ONE blocking
    /// thread. Verified live: this is the flow a plain Desktop endpoint
    /// actually answers — EMRS-enrolled users get an authorized identity;
    /// others get Bloomberg's precise reason (e.g. "User not in emrs
    /// userid=...").
    ///
    /// Thread confinement keeps every `!Send` SDK handle (`Identity`,
    /// `Request`, `Service`) on the thread that created it, matching
    /// xbbg-core's sourced threading contract, and keeps the async future
    /// `Send` for the binding layers. `//blp/apiauth` must already be open.
    fn classic_authorize_blocking<T>(
        &self,
        op: impl FnOnce(&Self, &xbbg_core::Identity) -> Result<T, BlpError>,
    ) -> Result<T, BlpError> {
        // Step 1: token for the logged-in user.
        let token_rx = {
            let _lifecycle = self.shared.lifecycle.read();
            if self.is_shutting_down() {
                return Err(Self::shutdown_error());
            }
            let mut pending = self.shared.pending_token.lock();
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            *pending = Some(tx);
            if let Err(err) = self
                .session
                .generate_token(&CorrelationId::Int(IDENTITY_TOKEN_CID))
            {
                pending.take();
                return Err(err);
            }
            rx
        };
        let token = self
            .recv_classic_step(&token_rx, &self.shared.pending_token, "token generation")
            .map_err(hint_entitlement_requirements)?;

        // Step 2: authorization request against //blp/apiauth.
        let auth_service = self.session.get_service(APIAUTH_SERVICE)?;
        let mut request = auth_service.create_request("AuthorizationRequest")?;
        request.set_str("token", &token)?;
        let mut identity = self.session.create_identity()?;

        let auth_rx = {
            let _lifecycle = self.shared.lifecycle.read();
            if self.is_shutting_down() {
                return Err(Self::shutdown_error());
            }
            let mut pending = self.shared.pending_identity_auth.lock();
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            *pending = Some(tx);
            if let Err(err) = self.session.send_authorization_request(
                &request,
                &mut identity,
                &CorrelationId::Int(IDENTITY_CID),
            ) {
                pending.take();
                return Err(err);
            }
            rx
        };
        self.recv_classic_step(
            &auth_rx,
            &self.shared.pending_identity_auth,
            "authorization",
        )
        .map_err(hint_entitlement_requirements)?;
        let _lifecycle = self.shared.lifecycle.read();
        if self.is_shutting_down() {
            return Err(Self::shutdown_error());
        }

        op(self, &identity)
    }

    /// Run `op` with an authorized identity.
    ///
    /// Auth-configured engines authorize the SDK-owned session identity once
    /// and then materialize a fresh refcounted handle per call
    /// (`authorized_identity`), inline after the last await — no `!Send`
    /// value ever lives across an await, so the future stays `Send` for the
    /// binding layers. Engines without auth run the classic token flow on a
    /// dedicated blocking thread per call; only the `Send` result crosses
    /// back.
    async fn with_identity<T: Send + 'static>(
        self: &Arc<Self>,
        op: impl FnOnce(&Self, &xbbg_core::Identity) -> Result<T, BlpError> + Send + 'static,
    ) -> Result<T, BlpError> {
        match &self.config.auth {
            Some(auth_config) => {
                self.ensure_identity(auth_config).await?;
                let _lifecycle = self.shared.lifecycle.read();
                if self.is_shutting_down() {
                    return Err(Self::shutdown_error());
                }
                let identity = self
                    .session
                    .authorized_identity(&CorrelationId::Int(IDENTITY_CID))?;
                op(self, &identity)
            }
            None => {
                // The blocking thread cannot await; open the auth service
                // here first.
                self.ensure_service(APIAUTH_SERVICE).await?;
                // Serialize classic attempts: the pending-waiter slots hold
                // exactly one in-flight sender each.
                let _flow = self.identity_flow_lock.lock().await;
                let this = Arc::clone(self);
                tokio::task::spawn_blocking(move || this.classic_authorize_blocking(op))
                    .await
                    .map_err(|err| BlpError::Internal {
                        detail: format!("identity task failed to complete: {err}"),
                    })?
            }
        }
    }

    /// Seat type of the authorized identity (BPS / NONBPS / INVALID).
    pub(crate) async fn identity_seat_type(self: &Arc<Self>) -> Result<SeatType, BlpError> {
        self.with_identity(|_, identity| identity.seat_type()).await
    }

    /// Entitlement check against `service`, reporting failed EIDs.
    pub(crate) async fn identity_check_entitlements(
        self: &Arc<Self>,
        service: &str,
        eids: &[i32],
    ) -> Result<EntitlementCheck, BlpError> {
        self.ensure_service(service).await?;
        let service = service.to_string();
        let eids = eids.to_vec();
        self.with_identity(move |worker, identity| {
            let service = worker.session.get_service(&service)?;
            identity.check_entitlements(&service, &eids)
        })
        .await
    }

    /// Whether the authorized identity may access `service` at all.
    pub(crate) async fn identity_is_authorized(
        self: &Arc<Self>,
        service: &str,
    ) -> Result<bool, BlpError> {
        self.ensure_service(service).await?;
        let service = service.to_string();
        self.with_identity(move |worker, identity| {
            let service = worker.session.get_service(&service)?;
            Ok(identity.is_authorized(&service))
        })
        .await
    }

    fn shutdown_error() -> BlpError {
        BlpError::Internal {
            detail: "request worker is shut down".to_string(),
        }
    }

    fn is_shutting_down(&self) -> bool {
        self.shared.shutting_down.load(Ordering::Acquire)
    }

    /// Submit a unified request. Failures are delivered through `reply`;
    /// `Some(ticket)` is returned only when the request is in flight and
    /// cancellable.
    pub(crate) async fn submit(
        &self,
        request: PreparedRequest,
        reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
    ) -> Option<RequestTicket> {
        if self.is_shutting_down() {
            let _ = reply.send(Err(Self::shutdown_error()));
            return None;
        }
        let params = request.params();
        let t0 = Instant::now();
        if let Err(error) = self.ensure_service(&params.service).await {
            let _ = reply.send(Err(error));
            return None;
        }
        xbbg_log::debug!(
            worker_id = self.id,
            elapsed_us = t0.elapsed().as_micros(),
            "ensure_service"
        );

        xbbg_log::debug!(
            worker_id = self.id,
            shape = ?request.shape(),
            fields = ?params.fields,
            "creating request state"
        );
        let state = create_request_state(&request, reply);
        self.dispatch_prepared(request, state)
    }

    /// Submit a unified streaming request.
    pub(crate) async fn submit_stream(
        &self,
        request: PreparedRequest,
        stream: mpsc::Sender<Result<RecordBatch, BlpError>>,
    ) -> Option<RequestTicket> {
        if self.is_shutting_down() {
            send_stream_error(stream, Self::shutdown_error());
            return None;
        }
        let params = request.params();
        let fields = params.fields.clone().unwrap_or_default();
        let ticker = params.security.clone().unwrap_or_default();

        let state = match request.shape() {
            PlannedRequestShape::HistData(_) => {
                UnifiedRequestState::HistDataStream(HistDataStreamState::new(fields, stream))
            }
            PlannedRequestShape::IntradayBar => {
                UnifiedRequestState::IntradayBarStream(IntradayBarStreamState::new(ticker, stream))
            }
            PlannedRequestShape::IntradayTick => UnifiedRequestState::IntradayTickStream(
                IntradayTickStreamState::new(ticker, stream),
            ),
            _ => {
                send_stream_error(
                    stream,
                    BlpError::InvalidArgument {
                        detail: format!(
                            "Streaming not supported for extractor: {:?}",
                            params.extractor
                        ),
                    },
                );
                return None;
            }
        };

        if let Err(error) = self.ensure_service(&params.service).await {
            state.fail(error);
            return None;
        }

        self.dispatch_prepared(request, state)
    }

    /// Register `state` in the slab and send the Bloomberg request. State is
    /// inserted before `sendRequest` so a response arriving on the dispatcher
    /// thread before this returns still finds its slot.
    fn dispatch_prepared(
        &self,
        request: PreparedRequest,
        state: UnifiedRequestState,
    ) -> Option<RequestTicket> {
        let lifecycle = self.shared.lifecycle.read();
        if self.is_shutting_down() {
            state.fail(Self::shutdown_error());
            return None;
        }
        let generation = self.shared.next_generation();
        let key = self.shared.insert_slot(generation, state);
        let dispatch_key = DispatchKey::with_generation(key, generation);
        let cid = dispatch_key.to_correlation_id();

        let send_result = (|| -> Result<CorrelationId, BlpError> {
            let params = request.params();
            // Build the request from a short-lived service view borrowed from
            // this worker's session. Do not cache the SDK service handle;
            // Bloomberg owns it through the session.
            let service = self.session.get_service(&params.service)?;
            xbbg_log::debug!(
                worker_id = self.id,
                operation = %request.effective_operation(),
                securities = ?params.securities,
                start_date = ?params.start_date,
                end_date = ?params.end_date,
                "building request"
            );
            let blp_request = build_request_from_params(self.id, &service, &request)?;
            self.session.send_request_with_label(
                &blp_request,
                Some(&cid),
                params.request_id.as_deref(),
            )
        })();

        let actual_cid = match send_result {
            Ok(actual_cid) => actual_cid,
            Err(error) => {
                if let Some(slot) = self.shared.take_slot(dispatch_key) {
                    slot.cell.fail(error);
                }
                return None;
            }
        };

        if let Some(detail) = correlation_invariant_failure(dispatch_key, &actual_cid, key) {
            if let Some(slot) = self.shared.take_slot(dispatch_key) {
                slot.cell.fail(BlpError::Internal {
                    detail: detail.clone(),
                });
            }
            if let Err(cancel_error) = self.session.cancel(&actual_cid) {
                xbbg_log::error!(
                    worker_id = self.id,
                    error = %cancel_error,
                    actual_cid = ?actual_cid,
                    "failed to cancel request after correlation invariant violation"
                );
            }
            xbbg_log::error!(
                worker_id = self.id,
                actual_cid = ?actual_cid,
                detail = %detail,
                "quarantining request worker after correlation invariant violation"
            );
            drop(lifecycle);
            self.signal_shutdown();
            return None;
        }

        let params = request.params();
        xbbg_log::debug!(
            worker_id = self.id,
            key,
            service = %params.service,
            operation = %request.effective_operation(),
            "request sent"
        );
        Some(RequestTicket { key, generation })
    }

    /// Cancel an in-flight request (caller dropped the awaitable). The
    /// generation makes the CID single-use, so a recycled slot never aliases
    /// it; cancelling an already-completed ticket is a no-op.
    pub(crate) fn cancel_request(&self, ticket: RequestTicket) {
        let dispatch_key = DispatchKey::with_generation(ticket.key, ticket.generation);
        let Some(slot) = self.shared.take_slot(dispatch_key) else {
            return;
        };
        let cid = dispatch_key.to_correlation_id();

        if let Err(error) = self.session.cancel(&cid) {
            xbbg_log::warn!(
                worker_id = self.id,
                key = ticket.key,
                error = %error,
                "failed to cancel Bloomberg request; quarantining worker"
            );
            slot.cell.discard();
            self.shared.health.store(2, Ordering::Release);
            self.signal_shutdown();
            return;
        }

        // Caller cancelled: drop the state without replying.
        slot.cell.discard();
        xbbg_log::info!(
            worker_id = self.id,
            key = ticket.key,
            "cancelled Bloomberg request"
        );
    }

    /// Fail a request that exceeded the configured hard timeout.
    pub(crate) fn timeout_request(&self, ticket: RequestTicket, timeout_ms: u64) {
        let dispatch_key = DispatchKey::with_generation(ticket.key, ticket.generation);
        let Some(slot) = self.shared.take_slot(dispatch_key) else {
            return;
        };
        let cid = dispatch_key.to_correlation_id();
        if let Err(error) = self.session.cancel(&cid) {
            xbbg_log::warn!(
                worker_id = self.id,
                key = ticket.key,
                error = %error,
                "timeout_request: Bloomberg cancel failed; quarantining worker"
            );
            slot.cell.fail(BlpError::Timeout);
            self.shared.health.store(2, Ordering::Release);
            self.signal_shutdown();
            return;
        }
        slot.cell.fail(BlpError::Timeout);
        xbbg_log::warn!(
            worker_id = self.id,
            key = ticket.key,
            timeout_ms = timeout_ms,
            "request exceeded request_timeout_ms; failed with BlpError::Timeout"
        );
    }

    /// Mark slow requests and return hard-timeout candidates for
    /// [`AsyncRequestWorker::timeout_request`].
    pub(crate) fn scan_timeouts(&self, hard_timeout: Option<Duration>) -> Vec<RequestTicket> {
        self.shared.scan_timeouts(hard_timeout)
    }

    /// Introspect a service's schema.
    pub(crate) async fn introspect_schema(
        &self,
        service_uri: &str,
    ) -> Result<crate::schema::ServiceSchema, BlpError> {
        xbbg_log::debug!(worker_id = self.id, service = %service_uri, "introspecting schema");

        self.ensure_service(service_uri).await?;
        let _lifecycle = self.shared.lifecycle.read();
        if self.is_shutting_down() {
            return Err(Self::shutdown_error());
        }

        let service = self.session.get_service(service_uri)?;
        let schema = crate::schema::introspect_service(&service, service_uri);

        xbbg_log::debug!(
            worker_id = self.id,
            service = %service_uri,
            operations = schema.operations.len(),
            "schema introspection complete"
        );

        Ok(schema)
    }

    /// Begin stopping the session without blocking (Drop-friendly).
    pub(crate) fn signal_shutdown(&self) {
        if self.stop_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let _lifecycle = self.shared.lifecycle.write();
        self.shared.shutting_down.store(true, Ordering::Release);
        self.shared.health.store(2, Ordering::Release);
        self.shared
            .fail_pending_service_opens("request worker shutdown");
        self.shared.fail_pending_identity("request worker shutdown");
        self.shared.drain_in_flight("request worker shutdown");
        self.session.stop_async();
    }

    /// Stop the session (blocks until in-flight callbacks drain) and fail
    /// anything still registered.
    pub(crate) fn shutdown_blocking(&self) {
        self.stop_started.store(true, Ordering::Release);
        let _lifecycle = self.shared.lifecycle.write();
        self.shared.shutting_down.store(true, Ordering::Release);
        self.shared.health.store(2, Ordering::Release);
        xbbg_log::info!(worker_id = self.id, "AsyncRequestWorker shutting down");
        self.session.stop();
        self.shared.fail_pending_service_opens("worker shutdown");
        self.shared.fail_pending_identity("worker shutdown");
        self.shared.drain_in_flight("worker shutdown");
    }
}

/// Create the appropriate request state based on the prepared request kind.
fn create_request_state(
    request: &PreparedRequest,
    reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
) -> UnifiedRequestState {
    let params = request.params();
    let fields = params.fields.clone().unwrap_or_default();
    let field_types = params.field_types.clone();

    match request.shape() {
        PlannedRequestShape::RefData(output) => {
            UnifiedRequestState::RefData(RefDataState::with_format(
                fields,
                output.format,
                output.long_mode,
                field_types,
                params.include_security_errors,
                reply,
            ))
        }
        PlannedRequestShape::HistData(output) => UnifiedRequestState::HistData(
            HistDataState::with_format(fields, output.format, output.long_mode, field_types, reply),
        ),
        PlannedRequestShape::BulkData => {
            let field = fields.first().cloned().unwrap_or_default();
            UnifiedRequestState::BulkData(BulkDataState::new(field, reply))
        }
        PlannedRequestShape::Generic => UnifiedRequestState::Generic(GenericState::new(reply)),
        PlannedRequestShape::Bql => UnifiedRequestState::Bql(BqlState::new(reply)),
        PlannedRequestShape::Bsrch => UnifiedRequestState::Bsrch(BsrchState::new(reply)),
        PlannedRequestShape::FieldInfo => {
            UnifiedRequestState::FieldInfo(FieldInfoState::new(reply))
        }
        PlannedRequestShape::IntradayBar => {
            // IntradayBarRequest has no column-adding elements (maxDataPoints,
            // gapFillInitialBar, adjustment*, etc. are behavior-only). The response
            // shape is always `barData.barTickData[]` with the same fields.
            let ticker = params.security.clone().unwrap_or_default();
            let event_type = params
                .event_type
                .clone()
                .unwrap_or_else(|| "TRADE".to_string());
            let interval = params.interval.unwrap_or(1);
            UnifiedRequestState::IntradayBar(IntradayBarState::new(
                ticker, event_type, interval, reply,
            ))
        }
        PlannedRequestShape::IntradayTick => {
            let ticker = params.security.clone().unwrap_or_default();
            UnifiedRequestState::IntradayTick(IntradayTickState::new(ticker, reply))
        }
    }
}

/// Build a Bloomberg request from a prepared request.
fn build_request_from_params(
    worker_id: usize,
    service: &xbbg_core::Service<'_>,
    prepared: &PreparedRequest,
) -> Result<xbbg_core::Request, BlpError> {
    let params = prepared.params();
    let operation = prepared.effective_operation();
    xbbg_log::trace!(operation = %operation, "creating request");
    let mut request = service.create_request(operation)?;
    xbbg_log::trace!("request created");

    if prepared.is_excel_get_grid_request() {
        apply_excel_grid_request_parameters(&mut request, params)?;
        return Ok(request);
    }

    // Set securities (multi or single)
    if let Some(securities) = &params.securities {
        for sec in securities {
            xbbg_log::trace!(element = "securities", value = %sec, "appending");
            request.append_str("securities", sec)?;
        }
    }
    if let Some(security) = &params.security {
        // Bloomberg intraday operations use a scalar "security" element; other
        // operations treat the singular convenience input as one "securities" entry.
        if prepared.uses_intraday_security_element() {
            xbbg_log::trace!(element = "security", value = %security, "setting scalar");
            request.set_str("security", security)?;
        } else {
            xbbg_log::trace!(element = "securities", value = %security, "appending");
            request.append_str("securities", security)?;
        }
    }

    // Set fields
    if let Some(fields) = &params.fields {
        for field in fields {
            xbbg_log::trace!(element = "fields", value = %field, "appending");
            request.append_str("fields", field)?;
        }
    }

    // Set date range (for historical) - scalar elements use set_str
    if let Some(start) = &params.start_date {
        xbbg_log::trace!(element = "startDate", value = %start, "setting");
        request.set_str("startDate", start)?;
    }
    if let Some(end) = &params.end_date {
        xbbg_log::trace!(element = "endDate", value = %end, "setting");
        request.set_str("endDate", end)?;
    }

    // Set datetime range (for intraday) - use proper datetime type
    if let Some(start) = &params.start_datetime {
        xbbg_log::trace!(element = "startDateTime", value = %start, "setting datetime");
        request.set_datetime("startDateTime", start)?;
    }
    if let Some(end) = &params.end_datetime {
        xbbg_log::trace!(element = "endDateTime", value = %end, "setting datetime");
        request.set_datetime("endDateTime", end)?;
    }

    // Set event type (singular, for intraday bars)
    if let Some(event_type) = &params.event_type {
        request.set_str("eventType", event_type)?;
    }
    // Set event types (array, for intraday ticks)
    if let Some(event_types) = &params.event_types {
        for et in event_types {
            xbbg_log::trace!(element = "eventTypes", value = %et, "appending event type");
            request.append_str("eventTypes", et)?;
        }
    }
    // Set interval (for intraday bars)
    if let Some(interval) = params.interval {
        request.set_int("interval", interval as i32)?;
    }

    // Apply generic request parameters from both `elements` and request-level `options`.
    // - Dotted paths (e.g., "priceSource.securityName") use nested setters
    // - Non-dotted names try scalar set first, fall back to append for arrays
    for (name, value) in iter_named_request_parameters(params) {
        apply_named_request_parameter(&mut request, name, value)?;
    }

    // First-class returnEids flag (validated to ReferenceData/HistoricalData
    // in request_plan). Same wire form as the element spelling, so an explicit
    // ("returnEids", "true") element stays consistent with this overwrite.
    if params.return_eids {
        xbbg_log::trace!(element = "returnEids", "setting");
        apply_named_request_parameter(&mut request, "returnEids", "true")?;
    }

    // Set apiflds field IDs
    if let Some(field_ids) = &params.field_ids {
        for id in field_ids {
            request.append_str("id", id)?;
        }
    }

    // Set overrides (fieldId/value pairs on the "overrides" sequence element)
    if let Some(overrides) = &params.overrides {
        if !overrides.is_empty() {
            let overrides_ptr = request.get_or_create_element("overrides")?;
            for (field_id, value) in overrides {
                // SAFETY: overrides_ptr is a valid element obtained from
                // get_or_create_element above; entry_ptr is valid from append_element.
                let entry_ptr = unsafe { request.append_element(overrides_ptr)? };
                unsafe { request.set_element_string(entry_ptr, "fieldId", field_id)? };
                unsafe { request.set_element_string(entry_ptr, "value", value)? };
            }
            xbbg_log::debug!(
                worker_id = worker_id,
                count = overrides.len(),
                "overrides applied"
            );
        }
    }

    // Set search spec (for FieldSearchRequest)
    if let Some(search_spec) = &params.search_spec {
        request.set_str("searchSpec", search_spec)?;
    }

    Ok(request)
}

fn extract_reason_description(msg: &xbbg_core::Message<'_>) -> Option<String> {
    let reason = msg.elements().get_by_str("reason")?;
    for key in ["description", "category", "message"] {
        if let Some(s) = reason.get_by_str(key).and_then(|e| e.get_str(0)) {
            return Some(s.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::RequestParams;

    fn shared() -> WorkerShared {
        WorkerShared::new(0, Arc::new(AtomicU8::new(0)))
    }

    fn generic_state() -> (
        UnifiedRequestState,
        oneshot::Receiver<Result<RecordBatch, BlpError>>,
    ) {
        let (tx, rx) = oneshot::channel();
        (UnifiedRequestState::Generic(GenericState::new(tx)), rx)
    }

    #[test]
    fn take_slot_requires_matching_generation() {
        let shared = shared();
        let (state, _rx) = generic_state();
        let generation = shared.next_generation();
        let key = shared.insert_slot(generation, state);

        // Wrong generation: slot stays.
        let stale = DispatchKey::with_generation(key, generation.wrapping_add(1));
        assert!(shared.take_slot(stale).is_none());
        assert_eq!(shared.requests.lock().len(), 1);

        // Right generation: slot removed.
        let fresh = DispatchKey::with_generation(key, generation);
        assert!(shared.take_slot(fresh).is_some());
        assert!(shared.requests.lock().is_empty());

        // Removing again is a no-op.
        assert!(shared.take_slot(fresh).is_none());
    }

    #[test]
    fn recycled_slot_is_not_visible_to_old_ticket() {
        let shared = shared();
        let (first, _rx1) = generic_state();
        let g1 = shared.next_generation();
        let key = shared.insert_slot(g1, first);
        let first_key = DispatchKey::with_generation(key, g1);
        assert!(shared.take_slot(first_key).is_some());

        // Recycle the same slab slot with a new generation.
        let (second, _rx2) = generic_state();
        let g2 = shared.next_generation();
        let key2 = shared.insert_slot(g2, second);
        assert_eq!(key, key2, "slab should recycle the slot");

        // The old ticket no longer addresses the slot.
        assert!(shared.take_slot(first_key).is_none());
        assert_eq!(shared.requests.lock().len(), 1);
    }

    #[test]
    fn scan_timeouts_warns_once_and_reports_expired() {
        let shared = shared();
        let (state, _rx) = generic_state();
        let generation = shared.next_generation();
        let key = shared.insert_slot(generation, state);

        // Backdate the slot beyond both thresholds.
        shared.requests.lock().get_mut(key).unwrap().sent_at =
            Instant::now() - Duration::from_secs(120);

        let expired = shared.scan_timeouts(Some(Duration::from_secs(60)));
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].key, key);
        assert_eq!(expired[0].generation, generation);
        assert!(shared.requests.lock().get(key).unwrap().warned);

        // Without a hard timeout nothing is reported, and the warning does
        // not repeat.
        let expired = shared.scan_timeouts(None);
        assert!(expired.is_empty());
    }

    #[test]
    fn drain_in_flight_fails_all_slots() {
        let shared = shared();
        let (s1, mut rx1) = generic_state();
        let (s2, mut rx2) = generic_state();
        let g1 = shared.next_generation();
        let g2 = shared.next_generation();
        shared.insert_slot(g1, s1);
        shared.insert_slot(g2, s2);

        shared.drain_in_flight("test drain");

        assert!(shared.requests.lock().is_empty());
        assert!(matches!(rx1.try_recv(), Ok(Err(BlpError::Internal { .. }))));
        assert!(matches!(rx2.try_recv(), Ok(Err(BlpError::Internal { .. }))));
    }

    #[test]
    fn startup_latch_resolves_once() {
        let shared = shared();
        shared.resolve_startup(Ok(()));
        shared.resolve_startup(Err(BlpError::Timeout)); // ignored: already resolved
        assert!(shared.wait_startup(Duration::from_millis(10)).is_ok());
    }

    #[test]
    fn startup_latch_times_out_when_unresolved() {
        let shared = shared();
        assert!(matches!(
            shared.wait_startup(Duration::from_millis(10)),
            Err(BlpError::Timeout)
        ));
    }

    #[test]
    fn iter_named_request_parameters_includes_options_after_elements() {
        let params = RequestParams {
            elements: Some(vec![(
                "periodicitySelection".to_string(),
                "DAILY".to_string(),
            )]),
            options: Some(vec![("returnEids".to_string(), "true".to_string())]),
            ..Default::default()
        };

        let collected: Vec<(String, String)> = iter_named_request_parameters(&params)
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();

        assert_eq!(
            collected,
            vec![
                ("periodicitySelection".to_string(), "DAILY".to_string()),
                ("returnEids".to_string(), "true".to_string()),
            ]
        );
    }

    #[test]
    fn correlation_invariant_rejects_foreign_and_non_dispatch_ids() {
        let expected = DispatchKey::with_generation(3, 7);
        assert!(
            correlation_invariant_failure(expected, &expected.to_correlation_id(), 3).is_none()
        );

        let foreign = DispatchKey::with_generation(4, 7).to_correlation_id();
        assert!(correlation_invariant_failure(expected, &foreign, 3).is_some());
        assert!(correlation_invariant_failure(expected, &CorrelationId::Unset, 3).is_some());
    }

    #[test]
    fn cancelled_service_waiter_removes_its_empty_generation() {
        let shared = shared();
        let service = "//blp/refdata";
        let attempt_cid = SERVICE_OPEN_CID_TAG | 1;
        let (tx, rx) = oneshot::channel();
        shared.pending_service_opens.lock().insert(
            service.to_string(),
            PendingServiceOpen {
                cid: attempt_cid,
                waiters: vec![tx],
            },
        );

        drop(PendingServiceWaiter::new(&shared, service, attempt_cid, rx));

        assert!(!shared.pending_service_opens.lock().contains_key(service));
    }

    #[test]
    fn cancelled_identity_waiter_is_pruned_immediately() {
        let shared = shared();
        let (tx, rx) = oneshot::channel();
        *shared.identity.lock() = IdentityAuthState::Pending(vec![tx]);

        drop(PendingIdentityWaiter::new(&shared, rx));

        match &*shared.identity.lock() {
            IdentityAuthState::Pending(waiters) => assert!(waiters.is_empty()),
            _ => panic!("pending identity generation must remain"),
        };
    }
}
