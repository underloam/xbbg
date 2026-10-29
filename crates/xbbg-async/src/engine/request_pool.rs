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

use super::state::ResponseMetadata;
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

/// Enrich only data-request outcomes; pre-dispatch failures have no SDK ID.
fn enrich_request_error(
    mut error: BlpError,
    service: &str,
    operation: &str,
    request_id: Option<&str>,
    ids: &[Arc<str>],
) -> BlpError {
    match &mut error {
        BlpError::RequestFailure {
            request_id: label, ..
        }
        | BlpError::RequestTimeout {
            request_id: label, ..
        } => {
            if label.is_none() {
                *label = request_id.map(str::to_owned);
            }
            error.extend_bloomberg_request_ids(ids.iter().cloned());
            error
        }
        BlpError::Internal { .. } if !ids.is_empty() => BlpError::RequestFailure {
            service: service.to_owned(),
            operation: Some(operation.to_owned()),
            cid: None,
            label: None,
            request_id: request_id.map(str::to_owned),
            bloomberg_request_ids: ids.to_vec(),
            source: Some(Box::new(error)),
        },
        _ => error,
    }
}

pub(super) fn enrich_async_request_error(
    error: BlpAsyncError,
    params: &super::RequestParams,
    ids: &[Arc<str>],
) -> BlpAsyncError {
    match error {
        BlpAsyncError::Blp(error) => enrich_request_error(
            error,
            &params.service,
            params.effective_operation(),
            params.request_id.as_deref(),
            ids,
        )
        .into(),
        error if !ids.is_empty() => BlpError::RequestFailure {
            service: params.service.clone(),
            operation: Some(params.effective_operation().to_owned()),
            cid: None,
            label: None,
            request_id: params.request_id.clone(),
            bloomberg_request_ids: ids.to_vec(),
            source: Some(Box::new(error)),
        }
        .into(),
        error => error,
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
    bloomberg_request_id: Option<Arc<str>>,
    request_context: Option<(String, String, Option<String>)>,
    /// Identical chunk schemas share the final timezone-adjusted ID-bearing schema.
    metadata_schema: Option<(arrow_schema::SchemaRef, arrow_schema::SchemaRef)>,
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
            bloomberg_request_id: None,
            request_context: None,
            metadata_schema: None,
        }
    }

    pub(crate) fn with_output_timezone(mut self, timezone: Option<String>) -> Self {
        self.output_timezone = timezone;
        self.metadata_schema = None;
        self
    }

    fn finish(&mut self) {
        if let Some(guard) = &mut self.cancel_guard {
            guard.disarm();
        }
        self.cancel_guard = None;
    }

    fn map_item(&mut self, item: Result<RecordBatch, BlpError>) -> Result<RecordBatch, BlpError> {
        match item {
            Ok(mut batch) => {
                let source_schema = self.bloomberg_request_id.as_ref().map(|_| batch.schema());
                let cached_schema = match (&source_schema, &self.metadata_schema) {
                    (Some(schema), Some((source, attached)))
                        if Arc::ptr_eq(source, schema) || source.as_ref() == schema.as_ref() =>
                    {
                        Some(Arc::clone(attached))
                    }
                    _ => None,
                };
                let cache_miss = cached_schema.is_none();
                if let Some(timezone) = self.output_timezone.as_deref() {
                    batch = super::intraday_timezone::apply_output_timezone_batch(
                        batch,
                        timezone,
                        cached_schema,
                    )
                    .map_err(|error| BlpError::Internal {
                        detail: format!("intraday output timezone: {error}"),
                    })?;
                } else if let Some(schema) = cached_schema {
                    batch = batch.with_schema(schema).expect("same fields");
                }
                if cache_miss
                    && let (Some(id), Some(source_schema)) =
                        (&self.bloomberg_request_id, source_schema)
                {
                    batch = ResponseMetadata::attach_bloomberg_request_id(batch, id);
                    self.metadata_schema = Some((source_schema, batch.schema()));
                }
                Ok(batch)
            }
            Err(error) => match &self.request_context {
                Some((service, operation, label)) => Err(enrich_request_error(
                    error,
                    service,
                    operation,
                    label.as_deref(),
                    self.bloomberg_request_id.as_slice(),
                )),
                None => Err(error),
            },
        }
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
                BlpAsyncError::Blp(BlpError::Internal {
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
        let mut bloomberg_request_ids = smallvec::SmallVec::<[Arc<str>; 1]>::new();

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
            let submission = worker.submit(&request, reply_tx).await;
            if let Some(id) = submission.bloomberg_request_id {
                bloomberg_request_ids.push(id);
            }
            let mut cancel_guard = submission
                .ticket
                .map(|ticket| RequestCancelGuard::new(Arc::clone(&worker), ticket));

            let reply_result = reply_rx.await;
            if let Some(guard) = &mut cancel_guard {
                guard.disarm();
            }

            match reply_result {
                Ok(Ok(batch)) => {
                    return Ok(match bloomberg_request_ids.as_slice() {
                        [] => batch,
                        [id] => ResponseMetadata::attach_bloomberg_request_id(batch, id),
                        _ => ResponseMetadata::with_bloomberg_request_ids(bloomberg_request_ids)
                            .attach(batch),
                    });
                }
                Ok(Err(error)) if self.is_retryable(&error) && attempt + 1 < max_attempts => {
                    last_error = Some(BlpAsyncError::Blp(error));
                    continue;
                }
                Ok(Err(error)) => {
                    return Err(BlpAsyncError::Blp(enrich_request_error(
                        error,
                        &params.service,
                        request.effective_operation(),
                        params.request_id.as_deref(),
                        &bloomberg_request_ids,
                    )));
                }
                Err(_) if attempt + 1 < max_attempts => {
                    last_error = Some(BlpAsyncError::ChannelClosed);
                    continue;
                }
                Err(_) => {
                    return Err(enrich_async_request_error(
                        BlpAsyncError::ChannelClosed,
                        params,
                        &bloomberg_request_ids,
                    ));
                }
            }
        }

        Err(enrich_async_request_error(
            last_error.unwrap_or(BlpAsyncError::ChannelClosed),
            params,
            &bloomberg_request_ids,
        ))
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
        let submission = worker.submit_stream(&request, stream_tx).await;
        let cancel_guard = submission
            .ticket
            .map(|ticket| RequestCancelGuard::new(Arc::clone(&worker), ticket));
        let mut stream = RequestStream::new(stream_rx, cancel_guard);
        stream.bloomberg_request_id = submission.bloomberg_request_id;
        stream.request_context = Some((
            params.service.clone(),
            request.effective_operation().to_owned(),
            params.request_id.clone(),
        ));
        Ok(stream)
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
            .map_err(BlpAsyncError::Blp)
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

    #[test]
    fn stream_preserves_sdk_id_labels_buffers_and_shared_schema() {
        use super::super::state::METADATA_KEY_BLOOMBERG_REQUEST_ID;
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "row",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let source = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int64Array::from(vec![1_i64, 2]))],
        )
        .unwrap();
        let (tx, rx) = mpsc::channel(3);
        tx.try_send(Ok(source.clone())).unwrap();
        tx.try_send(Ok(source.clone())).unwrap();
        tx.try_send(Err(BlpError::RequestTimeout {
            request_id: None,
            bloomberg_request_ids: Vec::new(),
        }))
        .unwrap();
        let mut stream = RequestStream::new(rx, None);
        stream.bloomberg_request_id = Some(Arc::from("sdk-synthetic-stream"));
        stream.request_context = Some((
            "//synthetic/refdata".into(),
            "SyntheticRequest".into(),
            Some("req-label".into()),
        ));
        let first = stream.try_recv().unwrap().unwrap();
        let second = stream.try_recv().unwrap().unwrap();
        assert_eq!(
            first.schema_ref().metadata()[METADATA_KEY_BLOOMBERG_REQUEST_ID],
            "sdk-synthetic-stream"
        );
        assert!(Arc::ptr_eq(first.schema_ref(), second.schema_ref()));
        assert!(Arc::ptr_eq(first.column(0), source.column(0)));
        assert!(!first.schema_ref().metadata().contains_key("request_id"));
        let error = stream.try_recv().unwrap().unwrap_err();
        match error {
            BlpError::RequestTimeout {
                request_id,
                bloomberg_request_ids,
            } => {
                assert_eq!(request_id.as_deref(), Some("req-label"));
                assert_eq!(
                    bloomberg_request_ids.as_slice(),
                    [Arc::<str>::from("sdk-synthetic-stream")]
                );
            }
            other => panic!("expected request timeout, got {other:?}"),
        }
    }

    #[test]
    fn non_utc_stream_shares_final_schema_and_preserves_timestamp_buffers() {
        use super::super::state::METADATA_KEY_BLOOMBERG_REQUEST_ID;
        use arrow_array::{Array, ArrayRef, Int64Array, TimestampMicrosecondArray};
        use arrow_schema::{DataType, Schema, TimeUnit};

        let instants = ["2024-11-03T05:30:00Z", "2024-11-03T06:30:00Z"].map(|text| {
            chrono::DateTime::parse_from_rfc3339(text)
                .unwrap()
                .timestamp_micros()
        });
        let first_source = RecordBatch::try_from_iter(vec![
            (
                "time",
                Arc::new(
                    TimestampMicrosecondArray::from(vec![
                        Some(instants[0]),
                        None,
                        Some(instants[1]),
                    ])
                    .with_timezone("UTC"),
                ) as ArrayRef,
            ),
            (
                "value",
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef,
            ),
        ])
        .unwrap();
        let second_source = RecordBatch::try_new(
            first_source.schema(),
            vec![
                Arc::new(
                    TimestampMicrosecondArray::from(vec![
                        Some(instants[1]),
                        None,
                        Some(instants[0]),
                    ])
                    .with_timezone("UTC"),
                ) as ArrayRef,
                Arc::clone(first_source.column(1)),
            ],
        )
        .unwrap();
        let equal_schema_source = second_source
            .clone()
            .with_schema(Arc::new(Schema::clone(second_source.schema_ref())))
            .unwrap();
        assert!(!Arc::ptr_eq(
            second_source.schema_ref(),
            equal_schema_source.schema_ref()
        ));
        let sources = [first_source, second_source, equal_schema_source];
        let (tx, rx) = mpsc::channel(sources.len());
        for source in &sources {
            tx.try_send(Ok(source.clone())).unwrap();
        }
        let mut stream =
            RequestStream::new(rx, None).with_output_timezone(Some("America/New_York".into()));
        stream.bloomberg_request_id = Some(Arc::from("sdk-synthetic-intraday"));
        let outputs: Vec<_> = sources
            .iter()
            .map(|_| stream.try_recv().unwrap().unwrap())
            .collect();

        for (source, output) in sources.iter().zip(&outputs) {
            assert!(Arc::ptr_eq(outputs[0].schema_ref(), output.schema_ref()));
            assert_eq!(
                output.schema_ref().metadata()[METADATA_KEY_BLOOMBERG_REQUEST_ID],
                "sdk-synthetic-intraday"
            );
            assert_eq!(
                output.schema_ref().field(0).data_type(),
                &DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into()))
            );
            let times = output
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let source_times = source
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            assert_eq!(times.data_type(), output.schema_ref().field(0).data_type());
            assert_eq!(times.values(), source_times.values());
            assert_eq!(times.values().as_ptr(), source_times.values().as_ptr());
            assert_eq!(
                times.nulls().unwrap().buffer().as_ptr(),
                source_times.nulls().unwrap().buffer().as_ptr()
            );
            assert!(times.is_null(1));
            assert!(Arc::ptr_eq(output.column(1), source.column(1)));
            assert_eq!(source_times.timezone(), Some("UTC"));
        }
        let times = outputs[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let timezone = times.timezone().unwrap().parse().unwrap();
        assert_eq!(
            times
                .value_as_datetime_with_tz(0, timezone)
                .unwrap()
                .to_rfc3339(),
            "2024-11-03T01:30:00-04:00"
        );
        assert_eq!(
            times
                .value_as_datetime_with_tz(2, timezone)
                .unwrap()
                .to_rfc3339(),
            "2024-11-03T01:30:00-05:00"
        );
    }

    #[test]
    fn non_utc_stream_preserves_chunk_diagnostics_and_schema_changes() {
        use super::super::state::{METADATA_KEY_BLOOMBERG_REQUEST_ID, METADATA_KEY_TYPE_MISSES};
        use arrow_array::{ArrayRef, Int64Array, TimestampMicrosecondArray};
        use arrow_schema::{DataType, TimeUnit};

        let make_batch = |field, misses| {
            let mut batch = RecordBatch::try_from_iter(vec![
                (
                    "time",
                    Arc::new(
                        TimestampMicrosecondArray::from(vec![Some(1_i64), None])
                            .with_timezone("UTC"),
                    ) as ArrayRef,
                ),
                (
                    field,
                    Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef,
                ),
            ])
            .unwrap();
            batch
                .schema_metadata_mut()
                .insert(METADATA_KEY_TYPE_MISSES.into(), misses);
            batch
        };
        let sources = [
            make_batch("value", r#"{"value":1}"#.to_string()),
            make_batch("value", r#"{"value":2}"#.to_string()),
            make_batch("changed", r#"{"changed":3}"#.to_string()),
        ];
        let (tx, rx) = mpsc::channel(sources.len());
        for source in &sources {
            tx.try_send(Ok(source.clone())).unwrap();
        }
        let mut stream =
            RequestStream::new(rx, None).with_output_timezone(Some("America/New_York".into()));
        stream.bloomberg_request_id = Some(Arc::from("sdk-synthetic-intraday"));
        let outputs: Vec<_> = sources
            .iter()
            .map(|_| stream.try_recv().unwrap().unwrap())
            .collect();

        for (source, output) in sources.iter().zip(&outputs) {
            assert_eq!(
                output.schema_ref().metadata()[METADATA_KEY_TYPE_MISSES],
                source.schema_ref().metadata()[METADATA_KEY_TYPE_MISSES]
            );
            assert_eq!(
                output.schema_ref().metadata()[METADATA_KEY_BLOOMBERG_REQUEST_ID],
                "sdk-synthetic-intraday"
            );
            assert_eq!(
                output.schema_ref().field(0).data_type(),
                &DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into()))
            );
            assert_eq!(output.schema_ref().field(1), source.schema_ref().field(1));
            assert!(Arc::ptr_eq(output.column(1), source.column(1)));
        }
        assert!(!Arc::ptr_eq(
            outputs[0].schema_ref(),
            outputs[1].schema_ref()
        ));
        assert!(!Arc::ptr_eq(
            outputs[1].schema_ref(),
            outputs[2].schema_ref()
        ));
        assert!(outputs[2].column_by_name("changed").is_some());
        assert!(outputs[2].column_by_name("value").is_none());
    }

    #[test]
    fn request_failures_keep_sdk_ids_distinct_from_existing_labels() {
        let error = BlpError::RequestFailure {
            service: "//synthetic/refdata".into(),
            operation: Some("SyntheticRequest".into()),
            cid: None,
            label: Some("synthetic failure".into()),
            request_id: Some("existing-label".into()),
            bloomberg_request_ids: vec![Arc::from("sdk-synthetic-a")],
            source: None,
        };
        let error = enrich_request_error(
            error,
            "//synthetic/refdata",
            "SyntheticRequest",
            Some("new-label"),
            &[Arc::from("sdk-synthetic-b"), Arc::from("sdk-synthetic-a")],
        );
        match error {
            BlpError::RequestFailure {
                request_id,
                bloomberg_request_ids,
                ..
            } => {
                assert_eq!(request_id.as_deref(), Some("existing-label"));
                assert_eq!(
                    bloomberg_request_ids,
                    vec![
                        Arc::<str>::from("sdk-synthetic-a"),
                        Arc::from("sdk-synthetic-b"),
                    ]
                );
            }
            other => panic!("expected request failure, got {other:?}"),
        }
        // REQUEST_STATUS and immediate send failures arrive as core Internal errors.
        let status_failure = enrich_request_error(
            BlpError::Internal {
                detail: "synthetic SDK failure".into(),
            },
            "//synthetic/refdata",
            "SyntheticRequest",
            Some("req-label"),
            &[Arc::from("sdk-synthetic-status")],
        );
        assert!(matches!(&status_failure, BlpError::RequestFailure { .. }));
        assert_eq!(
            status_failure.bloomberg_request_ids(),
            [Arc::<str>::from("sdk-synthetic-status")]
        );
        let (tx, rx) = mpsc::channel(1);
        tx.try_send(Ok(RecordBatch::new_empty(Arc::new(
            arrow_schema::Schema::empty(),
        ))))
        .unwrap();
        let mut stream = RequestStream::new(rx, None);
        assert!(
            stream
                .try_recv()
                .unwrap()
                .unwrap()
                .schema_ref()
                .metadata()
                .is_empty()
        );
    }

    #[test]
    fn lifecycle_timeout_keeps_its_classification_with_other_sdk_ids() {
        let error = enrich_request_error(
            BlpError::Timeout,
            "//synthetic/refdata",
            "SyntheticRequest",
            Some("req-label"),
            &[Arc::from("sdk-synthetic-prior-attempt")],
        );
        assert!(matches!(error, BlpError::Timeout));

        let params = super::super::RequestParams {
            service: "//synthetic/refdata".into(),
            operation: "SyntheticRequest".into(),
            request_id: Some("req-label".into()),
            ..Default::default()
        };
        let sibling_ids = [Arc::<str>::from("sdk-synthetic-sibling")];
        let error = enrich_async_request_error(BlpError::Timeout.into(), &params, &sibling_ids);
        assert!(matches!(error, BlpAsyncError::Blp(BlpError::Timeout)));

        let error = enrich_async_request_error(
            BlpError::RequestTimeout {
                request_id: None,
                bloomberg_request_ids: vec![Arc::from("sdk-synthetic-sent")],
            }
            .into(),
            &params,
            &sibling_ids,
        );
        match error {
            BlpAsyncError::Blp(BlpError::RequestTimeout {
                request_id,
                bloomberg_request_ids,
            }) => {
                assert_eq!(request_id.as_deref(), Some("req-label"));
                assert_eq!(
                    bloomberg_request_ids,
                    vec![
                        Arc::<str>::from("sdk-synthetic-sent"),
                        Arc::from("sdk-synthetic-sibling"),
                    ]
                );
            }
            other => panic!("expected sent request timeout, got {other:?}"),
        }
    }

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
