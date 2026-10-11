//! Standard-input transport for the stdio server.
//!
//! `rmcp::transport::stdio()` wraps `tokio::io::stdin()`, whose adapter reports every zero-byte
//! read as end-of-input and turns read errors into a silent `None`; the service loop then exits
//! with status 0 and nothing on stderr. Issue #348 is exactly that shape: on one Windows host the
//! server answers `initialize` and vanishes as soon as stdin goes quiet, and the bare exit left
//! nothing to diagnose. This reader runs the blocking reads on its own thread, only reports EOF
//! when stdin is really closed, and says on stderr why it stopped.

use std::fmt;
use std::io::{self, Read, Write};
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;

use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::{mpsc, oneshot};

const CHUNK_BYTES: usize = 64 * 1024;
const CHANNEL_CHUNKS: usize = 16;
const ZERO_READ_BACKOFF_MAX: Duration = Duration::from_millis(100);

/// Start the reader thread and return the async stream plus its termination monitor.
pub(crate) fn stdin() -> io::Result<(Stdin, StdinMonitor)> {
    let (tx, rx) = mpsc::channel(CHANNEL_CHUNKS);
    let (termination_tx, termination_rx) = oneshot::channel();
    thread::Builder::new()
        .name("xbbg-mcp-stdin".into())
        .spawn(move || {
            let termination = panic::catch_unwind(AssertUnwindSafe(|| {
                let mut stdin = io::stdin().lock();
                read_loop(&mut stdin, &tx)
            }))
            .unwrap_or_else(|_| {
                diagnostic(format_args!(
                    "xbbg-mcp: stdin reader thread panicked; shutting down"
                ));
                StdinTermination::ReaderPanicked
            });
            let _ = termination_tx.send(termination);
            drop(tx);
        })?;
    Ok((
        Stdin {
            rx,
            pending: Vec::new(),
            offset: 0,
        },
        StdinMonitor { termination_rx },
    ))
}

fn read_loop<R: Read>(stdin: &mut R, tx: &mpsc::Sender<io::Result<Vec<u8>>>) -> StdinTermination {
    let mut backoff = Duration::from_millis(1);
    let mut reported_zero_read = false;
    loop {
        let mut chunk = vec![0u8; CHUNK_BYTES];
        match stdin.read(&mut chunk) {
            Ok(0) => {
                if stdin_pipe_is_open() {
                    // A zero-byte read on a still-connected pipe is not end-of-input. Windows
                    // reports one after a zero-length write by the peer; `std` folds it into
                    // `Ok(0)` and every layer above then shuts down. Wait briefly and read again.
                    if !reported_zero_read {
                        diagnostic(format_args!(
                            "xbbg-mcp: ignoring zero-byte read on open stdin pipe"
                        ));
                        reported_zero_read = true;
                    }
                    thread::sleep(backoff);
                    backoff = (backoff * 2).min(ZERO_READ_BACKOFF_MAX);
                    continue;
                }
                diagnostic(format_args!("xbbg-mcp: stdin closed; shutting down"));
                return StdinTermination::EndOfInput;
            }
            Ok(n) => {
                chunk.truncate(n);
                backoff = Duration::from_millis(1);
                if tx.blocking_send(Ok(chunk)).is_err() {
                    return StdinTermination::ServiceStopped;
                }
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => {
                diagnostic(format_args!(
                    "xbbg-mcp: stdin read failed: {err}; shutting down"
                ));
                let kind = err.kind();
                let message = err.to_string();
                let _ = tx.blocking_send(Err(io::Error::new(kind, message.clone())));
                return StdinTermination::ReadFailed(io::Error::new(kind, message));
            }
        }
    }
}

fn diagnostic(args: fmt::Arguments<'_>) {
    let mut stderr = io::stderr().lock();
    write_diagnostic(&mut stderr, args);
}

fn write_diagnostic(writer: &mut impl Write, args: fmt::Arguments<'_>) {
    let _ = writeln!(writer, "{args}");
}

/// `true` when stdin is a pipe whose write end is still open.
///
/// `PeekNamedPipe` succeeds (with zero bytes available) while a writer holds the other end and
/// fails with `ERROR_BROKEN_PIPE` once every writer has gone, so it separates "nothing to read
/// right now" from end-of-input where `ReadFile` alone does not.
#[cfg(windows)]
fn stdin_pipe_is_open() -> bool {
    use std::os::windows::io::AsRawHandle;
    use std::ptr;

    type Handle = *mut core::ffi::c_void;
    const FILE_TYPE_PIPE: u32 = 0x0003;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetFileType(handle: Handle) -> u32;
        fn PeekNamedPipe(
            handle: Handle,
            buffer: *mut core::ffi::c_void,
            buffer_size: u32,
            bytes_read: *mut u32,
            total_bytes_avail: *mut u32,
            bytes_left_this_message: *mut u32,
        ) -> i32;
    }

    let handle = io::stdin().as_raw_handle() as Handle;
    // SAFETY: both calls take a process-owned handle and, with a null buffer and zero size,
    // write nothing; every out-pointer is documented as optional.
    unsafe {
        GetFileType(handle) == FILE_TYPE_PIPE
            && PeekNamedPipe(
                handle,
                ptr::null_mut(),
                0,
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
            ) != 0
    }
}

/// POSIX `read` returns 0 on a pipe only at end-of-input.
#[cfg(not(windows))]
fn stdin_pipe_is_open() -> bool {
    false
}

#[derive(Debug)]
enum StdinTermination {
    EndOfInput,
    ReadFailed(io::Error),
    ReaderPanicked,
    ServiceStopped,
}

/// Out-of-band termination state because `rmcp` maps read failures and EOF to `Closed`.
pub(crate) struct StdinMonitor {
    termination_rx: oneshot::Receiver<StdinTermination>,
}

impl StdinMonitor {
    pub(crate) fn ensure_clean_shutdown(mut self) -> io::Result<()> {
        match self.termination_rx.try_recv() {
            Ok(StdinTermination::EndOfInput) => Ok(()),
            Ok(StdinTermination::ReadFailed(err)) => Err(err),
            Ok(StdinTermination::ReaderPanicked) => {
                Err(io::Error::other("stdin reader thread panicked"))
            }
            Ok(StdinTermination::ServiceStopped) => Err(io::Error::other(
                "stdin reader stopped because the MCP service dropped its input",
            )),
            Err(oneshot::error::TryRecvError::Empty) => Err(io::Error::other(
                "MCP transport closed while the stdin reader was still running",
            )),
            Err(oneshot::error::TryRecvError::Closed) => Err(io::Error::other(
                "stdin reader stopped without reporting why",
            )),
        }
    }
}
/// Async side of the stdin reader; feed it to `rmcp` in place of `tokio::io::Stdin`.
pub(crate) struct Stdin {
    rx: mpsc::Receiver<io::Result<Vec<u8>>>,
    pending: Vec<u8>,
    offset: usize,
}

impl AsyncRead for Stdin {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.offset >= self.pending.len() {
            match self.rx.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Ok(())), // EOF: reader thread finished
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Ready(Some(Ok(chunk))) => {
                    self.pending = chunk;
                    self.offset = 0;
                }
            }
        }
        let this = &mut *self;
        let available = &this.pending[this.offset..];
        let n = available.len().min(buf.remaining());
        buf.put_slice(&available[..n]);
        this.offset += n;
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    fn stream(chunks: Vec<io::Result<Vec<u8>>>) -> Stdin {
        let (tx, rx) = mpsc::channel(CHANNEL_CHUNKS);
        for chunk in chunks {
            tx.try_send(chunk).expect("channel has room");
        }
        Stdin {
            rx,
            pending: Vec::new(),
            offset: 0,
        }
    }

    #[tokio::test]
    async fn chunks_are_delivered_in_order_and_split_across_small_reads() {
        let mut stdin = stream(vec![Ok(b"hello ".to_vec()), Ok(b"world".to_vec())]);
        let mut small = [0u8; 4];
        let mut out = Vec::new();
        loop {
            let n = stdin.read(&mut small).await.unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&small[..n]);
        }
        assert_eq!(out, b"hello world");
    }

    #[tokio::test]
    async fn read_error_surfaces_before_eof() {
        let mut stdin = stream(vec![
            Ok(b"x".to_vec()),
            Err(io::Error::other("stdin handle invalidated")),
        ]);
        let mut buf = [0u8; 8];
        assert_eq!(stdin.read(&mut buf).await.unwrap(), 1);
        let err = stdin.read(&mut buf).await.unwrap_err();
        assert_eq!(err.to_string(), "stdin handle invalidated");
        assert_eq!(stdin.read(&mut buf).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn closed_channel_is_eof() {
        let mut stdin = stream(vec![]);
        let mut buf = [0u8; 8];
        assert_eq!(stdin.read(&mut buf).await.unwrap(), 0);
    }

    #[test]
    fn failed_diagnostic_write_is_ignored() {
        struct BrokenWriter;

        impl Write for BrokenWriter {
            fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
                Err(io::ErrorKind::BrokenPipe.into())
            }

            fn flush(&mut self) -> io::Result<()> {
                Err(io::ErrorKind::BrokenPipe.into())
            }
        }

        write_diagnostic(&mut BrokenWriter, format_args!("diagnostic"));
    }

    fn monitor(termination: StdinTermination) -> StdinMonitor {
        let (tx, termination_rx) = oneshot::channel();
        tx.send(termination).expect("monitor receiver is open");
        StdinMonitor { termination_rx }
    }

    #[test]
    fn monitor_accepts_real_end_of_input() {
        monitor(StdinTermination::EndOfInput)
            .ensure_clean_shutdown()
            .unwrap();
    }

    #[test]
    fn monitor_preserves_read_failure() {
        let err = monitor(StdinTermination::ReadFailed(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "stdin broke",
        )))
        .ensure_clean_shutdown()
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(err.to_string(), "stdin broke");
    }

    #[test]
    fn monitor_rejects_reader_panic() {
        let err = monitor(StdinTermination::ReaderPanicked)
            .ensure_clean_shutdown()
            .unwrap_err();
        assert_eq!(err.to_string(), "stdin reader thread panicked");
    }

    #[test]
    fn monitor_rejects_transport_close_while_reader_is_running() {
        let (_tx, termination_rx) = oneshot::channel();
        let err = StdinMonitor { termination_rx }
            .ensure_clean_shutdown()
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "MCP transport closed while the stdin reader was still running"
        );
    }
}
