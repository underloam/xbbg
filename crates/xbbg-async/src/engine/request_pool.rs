//! Request worker pool with round-robin dispatch.
//!
//! The pool manages a collection of pre-warmed async-session workers and
//! distributes incoming requests across them using round-robin scheduling.
//! Submissions call straight into the chosen worker (async Bloomberg
//! sessions are thread-safe), so there is no command queue; a single
//! timeout-scanner thread enforces `request_timeout_ms` across all workers.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use std::time::Duration;

use arrow_array::RecordBatch;
use futures_util::Stream;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};

use xbbg_core::BlpError;

use super::worker::{AsyncRequestWorker, RequestTicket};
use super::{BlpAsyncError, EngineConfig, PreparedRequest, WorkerHealth};

/// How often the scanner thread sweeps workers for slow/expired requests.
const TIMEOUT_SCAN_INTERVAL: Duration = Duration::from_secs(1);

/// Polling granularity for scanner shutdown responsiveness.
const TIMEOUT_SCAN_TICK: Duration = Duration::from_millis(100);

/// Cancellation target retained by an in-flight request owner.
enum RequestCancellation {
    Worker {
        worker: Arc<AsyncRequestWorker>,
        ticket: RequestTicket,
    },
    #[cfg(test)]
    Counter(Arc<AtomicUsize>),
}

impl RequestCancellation {
    fn cancel(self) {
        match self {
            Self::Worker { worker, ticket } => worker.cancel_request(ticket),
            #[cfg(test)]
            Self::Counter(counter) => {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Cancels the in-flight request when its future or stream is dropped before
/// Bloomberg finishes the response.
struct RequestCancelGuard {
    cancellation: Option<RequestCancellation>,
}

impl RequestCancelGuard {
    fn new(worker: Arc<AsyncRequestWorker>, ticket: RequestTicket) -> Self {
        Self {
            cancellation: Some(RequestCancellation::Worker { worker, ticket }),
        }
    }

    #[cfg(test)]
    fn with_counter(counter: Arc<AtomicUsize>) -> Self {
        Self {
            cancellation: Some(RequestCancellation::Counter(counter)),
        }
    }

    fn disarm(&mut self) {
        self.cancellation = None;
    }

    fn cancel(&mut self) {
        if let Some(cancellation) = self.cancellation.take() {
            cancellation.cancel();
        }
    }
}

impl Drop for RequestCancelGuard {
    fn drop(&mut self) {
        self.cancel();
    }
}

/// Owned streaming Bloomberg request.
///
/// The cancellation guard cannot be separated from the receiver: dropping or
/// closing the stream removes the request from its worker and sends Bloomberg
/// a cancellation when the response is still in flight.
pub struct RequestStream {
    rx: mpsc::Receiver<Result<RecordBatch, BlpError>>,
    cancel_guard: Option<RequestCancelGuard>,
    output_timezone: Option<String>,
}

impl RequestStream {
    fn new(
        rx: mpsc::Receiver<Result<RecordBatch, BlpError>>,
        cancel_guard: Option<RequestCancelGuard>,
    ) -> Self {
        Self {
            rx,
            cancel_guard,
            output_timezone: None,
        }
    }

    pub(crate) fn with_output_timezone(mut self, timezone: Option<String>) -> Self {
        self.output_timezone = timezone;
        self
    }

    fn finish(&mut self) {
        if let Some(guard) = &mut self.cancel_guard {
            guard.disarm();
        }
        self.cancel_guard = None;
    }

    fn map_item(&self, item: Result<RecordBatch, BlpError>) -> Result<RecordBatch, BlpError> {
        let Some(timezone) = self.output_timezone.as_deref() else {
            return item;
        };
        item.and_then(|batch| {
            super::intraday_timezone::apply_output_timezone_batch(batch, timezone).map_err(
                |error| BlpError::Internal {
                    detail: format!("intraday output timezone: {error}"),
                },
            )
        })
    }

    /// Receive the next response batch.
    pub async fn recv(&mut self) -> Option<Result<RecordBatch, BlpError>> {
        match self.rx.recv().await {
            Some(item) => Some(self.map_item(item)),
            None => {
                self.finish();
                None
            }
        }
    }

    /// Try to receive a response batch without waiting.
    pub fn try_recv(&mut self) -> Result<Result<RecordBatch, BlpError>, mpsc::error::TryRecvError> {
        match self.rx.try_recv() {
            Ok(item) => Ok(self.map_item(item)),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                self.finish();
                Err(mpsc::error::TryRecvError::Disconnected)
            }
            Err(error) => Err(error),
        }
    }

    /// Stop the Bloomberg request while retaining already-buffered batches for
    /// subsequent `recv`/`try_recv` calls.
    pub fn close(&mut self) {
        self.rx.close();
        if let Some(mut guard) = self.cancel_guard.take() {
            guard.cancel();
        }
    }
}

impl Stream for RequestStream {
    type Item = Result<RecordBatch, BlpError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.rx).poll_recv(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(this.map_item(item))),
            Poll::Ready(None) => {
                this.finish();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Background thread enforcing slow-request warnings and hard timeouts.
struct TimeoutScanner {
    stop: Arc<AtomicBool>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl TimeoutScanner {
    fn spawn(
        workers: Vec<Weak<AsyncRequestWorker>>,
        request_timeout_ms: u64,
    ) -> Result<Self, BlpAsyncError> {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let thread = std::thread::Builder::new()
            .name("xbbg-request-timeouts".to_string())
            .spawn(move || {
                let hard_timeout =
                    (request_timeout_ms > 0).then(|| Duration::from_millis(request_timeout_ms));
                let ticks_per_scan =
                    (TIMEOUT_SCAN_INTERVAL.as_millis() / TIMEOUT_SCAN_TICK.as_millis()).max(1);
                loop {
                    for _ in 0..ticks_per_scan {
                        if stop_flag.load(Ordering::Acquire) {
                            return;
                        }
                        std::thread::sleep(TIMEOUT_SCAN_TICK);
                    }
                    for worker in &workers {
                        let Some(worker) = worker.upgrade() else {
                            continue;
                        };
                        for ticket in worker.scan_timeouts(hard_timeout) {
                            worker.timeout_request(ticket, request_timeout_ms);
                        }
                    }
                }
            })
            .map_err(|error| {
                BlpAsyncError::Internal(format!(
                    "failed to spawn request timeout scanner thread: {error}"
                ))
            })?;
        Ok(Self {
            stop,
            thread: Mutex::new(Some(thread)),
        })
    }

    fn signal_stop(&self) {
        self.stop.store(true, Ordering::Release);
    }

    fn join(&self) {
        self.signal_stop();
        // Keep concurrent shutdown callers waiting until the join is complete.
        let mut thread = self.thread.lock();
        if let Some(thread) = thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for TimeoutScanner {
    fn drop(&mut self) {
        self.signal_stop();
    }
}

/// Pool of request workers with round-robin dispatch.
///
/// The public surface is limited to construction, health/introspection, and
/// shutdown. Request dispatch is intentionally crate-private because correct
/// preparation depends on [`super::Engine`]-owned schema, field-cache, and
/// intraday-timezone state.
pub struct RequestWorkerPool {
    /// Workers; `Arc` so cancel guards and the scanner can address them
    /// after the submitting borrow ends.
    workers: Vec<Arc<AsyncRequestWorker>>,
    /// Round-robin counter.
    next_worker: AtomicUsize,
    /// Monotonic pool lifecycle gate.
    shutting_down: AtomicBool,
    /// Configuration.
    config: Arc<EngineConfig>,
    /// Slow-request / hard-timeout enforcement. `None` only in unit tests.
    scanner: Option<TimeoutScanner>,
}

impl RequestWorkerPool {
    /// Create a new pool with the specified number of workers.
    ///
    /// Each worker owns a pre-warmed asynchronous Bloomberg session; creation
    /// blocks until every session has started (parity with the previous
    /// thread-per-worker design, which also blocked on session startup).
    pub fn new(size: usize, config: Arc<EngineConfig>) -> Result<Self, BlpAsyncError> {
        if size == 0 {
            return Err(BlpAsyncError::ConfigError {
                detail: "request_pool_size must be at least 1".to_string(),
            });
        }

        xbbg_log::info!(pool_size = size, "creating request worker pool");

        let mut workers = Vec::with_capacity(size);
        for id in 0..size {
            let worker = AsyncRequestWorker::new(id, config.clone()).map_err(|e| {
                BlpAsyncError::BlpError(BlpError::Internal {
                    detail: format!("failed to spawn worker {}: {}", id, e),
                })
            })?;
            workers.push(Arc::new(worker));
        }

        let scanner = TimeoutScanner::spawn(
            workers.iter().map(Arc::downgrade).collect(),
            config.request_timeout_ms,
        )?;

        xbbg_log::info!(pool_size = size, "request worker pool ready");

        Ok(Self {
            workers,
            next_worker: AtomicUsize::new(0),
            shutting_down: AtomicBool::new(false),
            config,
            scanner: Some(scanner),
        })
    }

    fn next_healthy_worker(&self) -> Result<&Arc<AsyncRequestWorker>, BlpAsyncError> {
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(BlpAsyncError::ConfigError {
                detail: "request worker pool is shut down".to_string(),
            });
        }
        let len = self.workers.len();
        let start = self.next_worker.fetch_add(1, Ordering::Relaxed) % len;

        for offset in 0..len {
            let idx = (start + offset) % len;
            let worker = &self.workers[idx];
            if worker.health() != WorkerHealth::Dead {
                return Ok(worker);
            }
        }

        Err(BlpAsyncError::AllWorkersDown { pool_size: len })
    }

    /// A healthy worker for out-of-band session operations (identity /
    /// entitlement queries). Same round-robin selection as request dispatch.
    pub(crate) fn any_healthy_worker(&self) -> Result<Arc<AsyncRequestWorker>, BlpAsyncError> {
        self.next_healthy_worker().map(Arc::clone)
    }

    fn retry_delay(&self, attempt: usize) -> u64 {
        if attempt == 0 {
            return 0;
        }

        let policy = &self.config.retry_policy;
        let exponent = (attempt - 1) as f64;
        let delay = (policy.initial_delay_ms as f64) * policy.backoff_factor.powf(exponent);
        let bounded = if delay.is_finite() {
            delay.min(policy.max_delay_ms as f64)
        } else {
            policy.max_delay_ms as f64
        };

        bounded.max(0.0).round() as u64
    }

    fn is_retryable(&self, error: &BlpError) -> bool {
        match error {
            BlpError::Internal { detail } => {
                let detail = detail.to_ascii_lowercase();
                detail.contains("session")
                    || detail.contains("connection")
                    || detail.contains("transport")
            }
            _ => false,
        }
    }

    /// Dispatch a prepared request to an available worker and wait for the result.
    pub(crate) async fn request(
        &self,
        request: PreparedRequest,
    ) -> Result<RecordBatch, BlpAsyncError> {
        let params = request.params();
        let max_attempts = 1 + self.config.retry_policy.max_retries as usize;
        let mut last_error = None;

        for attempt in 0..max_attempts {
            if attempt > 0 {
                let delay = self.retry_delay(attempt);
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                xbbg_log::info!(attempt = attempt, delay_ms = delay, "retrying request");
            }

            let worker = match self.next_healthy_worker() {
                Ok(worker) => Arc::clone(worker),
                Err(error) => {
                    last_error = Some(error);
                    continue;
                }
            };

            let (reply_tx, reply_rx) = oneshot::channel();

            xbbg_log::debug!(
                worker_id = worker.id,
                service = %params.service,
                operation = %params.operation,
                attempt = attempt,
                "dispatching request"
            );

            // Failures inside submit are routed through the reply channel;
            // a ticket means the request is in flight and cancellable.
            let ticket = worker.submit(request.clone(), reply_tx).await;
            let mut cancel_guard =
                ticket.map(|ticket| RequestCancelGuard::new(Arc::clone(&worker), ticket));

            let reply_result = reply_rx.await;
            if let Some(guard) = &mut cancel_guard {
                guard.disarm();
            }

            match reply_result {
                Ok(Ok(batch)) => return Ok(batch),
                Ok(Err(error)) if self.is_retryable(&error) && attempt + 1 < max_attempts => {
                    last_error = Some(BlpAsyncError::BlpError(error));
                    continue;
                }
                Ok(Err(error)) => return Err(BlpAsyncError::BlpError(error)),
                Err(_) if attempt + 1 < max_attempts => {
                    last_error = Some(BlpAsyncError::ChannelClosed);
                    continue;
                }
                Err(_) => return Err(BlpAsyncError::ChannelClosed),
            }
        }

        Err(last_error.unwrap_or(BlpAsyncError::ChannelClosed))
    }

    /// Dispatch a prepared streaming request to an available worker.
    ///
    /// The returned stream owns its cancellation guard. Dropping the stream
    /// cannot leave a decoder registered in the worker slab.
    pub(crate) async fn request_stream(
        &self,
        request: PreparedRequest,
    ) -> Result<RequestStream, BlpAsyncError> {
        let params = request.params();
        let (stream_tx, stream_rx) = mpsc::channel(self.config.subscription_stream_capacity);

        let worker = Arc::clone(self.next_healthy_worker()?);
        xbbg_log::debug!(
            worker_id = worker.id,
            service = %params.service,
            operation = %params.operation,
            "dispatching stream request"
        );
        let ticket = worker.submit_stream(request, stream_tx).await;
        let cancel_guard =
            ticket.map(|ticket| RequestCancelGuard::new(Arc::clone(&worker), ticket));

        Ok(RequestStream::new(stream_rx, cancel_guard))
    }

    /// Get the number of workers in the pool.
    pub fn size(&self) -> usize {
        self.workers.len()
    }

    pub fn worker_health(&self) -> Vec<(usize, WorkerHealth)> {
        self.workers
            .iter()
            .map(|worker| (worker.id, worker.health()))
            .collect()
    }

    /// Introspect a service's schema via a worker.
    pub async fn introspect_schema(
        &self,
        service: String,
    ) -> Result<crate::schema::ServiceSchema, BlpAsyncError> {
        let worker = self.next_healthy_worker()?;
        worker
            .introspect_schema(&service)
            .await
            .map_err(BlpAsyncError::BlpError)
    }

    /// Signal shutdown to all workers (non-blocking).
    ///
    /// Sessions begin stopping asynchronously; used by Drop to avoid blocking
    /// during interpreter shutdown.
    pub fn signal_shutdown(&self) {
        if self.shutting_down.swap(true, Ordering::AcqRel) {
            return;
        }
        xbbg_log::info!(
            pool_size = self.workers.len(),
            "signaling request pool shutdown"
        );
        if let Some(scanner) = &self.scanner {
            scanner.signal_stop();
        }
        for worker in &self.workers {
            worker.signal_shutdown();
        }
    }

    /// Graceful shutdown - waits for all workers' sessions to stop (blocking).
    ///
    /// Use this for clean shutdown when you can afford to wait.
    /// Shared owners may call this repeatedly; the scanner is joined once.
    pub fn shutdown_blocking(&self) {
        self.shutting_down.store(true, Ordering::Release);
        xbbg_log::info!(
            pool_size = self.workers.len(),
            "shutting down request pool (blocking)"
        );
        if let Some(scanner) = &self.scanner {
            scanner.join();
        }
        for worker in &self.workers {
            worker.shutdown_blocking();
        }
    }
}

impl Drop for RequestWorkerPool {
    fn drop(&mut self) {
        // Non-blocking: signal sessions to stop; AsyncSession::drop completes
        // the (already-initiated, hence brief) stop before destroying.
        self.signal_shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::RetryPolicy;

    fn pool_with_retry_policy(retry_policy: RetryPolicy) -> RequestWorkerPool {
        let config = Arc::new(EngineConfig {
            retry_policy,
            ..EngineConfig::default()
        });

        RequestWorkerPool {
            workers: Vec::new(),
            next_worker: AtomicUsize::new(0),
            shutting_down: AtomicBool::new(false),
            config,
            scanner: None,
        }
    }

    #[test]
    fn retry_delay_uses_exponential_backoff_and_max_delay() {
        let pool = pool_with_retry_policy(RetryPolicy {
            max_retries: 4,
            initial_delay_ms: 100,
            backoff_factor: 2.5,
            max_delay_ms: 600,
        });

        assert_eq!(pool.retry_delay(0), 0);
        assert_eq!(pool.retry_delay(1), 100);
        assert_eq!(pool.retry_delay(2), 250);
        assert_eq!(pool.retry_delay(3), 600);
        assert_eq!(pool.retry_delay(4), 600);
    }

    #[test]
    fn retry_delay_clamps_non_finite_backoff_to_max_delay() {
        let pool = pool_with_retry_policy(RetryPolicy {
            max_retries: 1,
            initial_delay_ms: u64::MAX,
            backoff_factor: f64::INFINITY,
            max_delay_ms: 750,
        });

        assert_eq!(pool.retry_delay(2), 750);
    }

    #[test]
    fn is_retryable_only_matches_transient_internal_errors() {
        let pool = pool_with_retry_policy(RetryPolicy::default());

        assert!(pool.is_retryable(&BlpError::Internal {
            detail: "session connection dropped".to_string(),
        }));
        assert!(pool.is_retryable(&BlpError::Internal {
            detail: "transport reset".to_string(),
        }));
        assert!(!pool.is_retryable(&BlpError::Internal {
            detail: "bad request shape".to_string(),
        }));
        assert!(!pool.is_retryable(&BlpError::InvalidArgument {
            detail: "invalid field".to_string(),
        }));
        assert!(!pool.is_retryable(&BlpError::Timeout));
    }

    fn stream_with_cancel_counter() -> (RequestStream, Arc<AtomicUsize>) {
        let (_tx, rx) = mpsc::channel(1);
        let counter = Arc::new(AtomicUsize::new(0));
        let guard = RequestCancelGuard::with_counter(Arc::clone(&counter));
        (RequestStream::new(rx, Some(guard)), counter)
    }

    #[test]
    fn dropping_request_stream_cancels_once() {
        let (stream, counter) = stream_with_cancel_counter();

        drop(stream);

        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn closing_request_stream_cancels_once_and_drop_does_not_repeat() {
        let (mut stream, counter) = stream_with_cancel_counter();

        stream.close();
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        drop(stream);

        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn closing_request_stream_preserves_buffered_batches_for_drain() {
        let (tx, rx) = mpsc::channel(1);
        tx.try_send(Ok(RecordBatch::new_empty(Arc::new(
            arrow_schema::Schema::empty(),
        ))))
        .expect("buffer batch");
        let counter = Arc::new(AtomicUsize::new(0));
        let guard = RequestCancelGuard::with_counter(Arc::clone(&counter));
        let mut stream = RequestStream::new(rx, Some(guard));

        stream.close();

        assert_eq!(counter.load(Ordering::Relaxed), 1);
        assert!(stream.try_recv().expect("buffered batch").is_ok());
        assert!(matches!(
            stream.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }
    #[tokio::test]
    async fn terminal_request_stream_disarms_cancellation() {
        let (tx, rx) = mpsc::channel(1);
        let counter = Arc::new(AtomicUsize::new(0));
        let guard = RequestCancelGuard::with_counter(Arc::clone(&counter));
        let mut stream = RequestStream::new(rx, Some(guard));
        drop(tx);

        assert!(stream.recv().await.is_none());
        drop(stream);

        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn shutdown_is_monotonic_and_rejects_dispatch() {
        let pool = pool_with_retry_policy(RetryPolicy::default());

        pool.signal_shutdown();
        pool.signal_shutdown();

        let error = match pool.next_healthy_worker() {
            Ok(_) => panic!("shutdown pool must reject worker selection"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("pool is shut down"));
    }
}
