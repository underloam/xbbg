# xbbg-log

Zero-GIL logging infrastructure for the xbbg workspace.

Provides a `tracing`-based setup designed for Rust-Python hybrid libraries where worker threads must never acquire the GIL. Python controls the log level via a single atomic store — no locks, no GIL, no contention.

## Why this crate exists

The standard `pyo3-log` bridge acquires the GIL on every log event to forward to Python's `logging` module. In xbbg, worker threads poll Bloomberg at 10ms intervals — GIL contention from logging caused measurable latency. This crate eliminates that entirely.

## Architecture

```text
tracing::debug!("...")
  → AtomicLevelFilter (reads AtomicU8, ~1ns, zero GIL)
  → fmt::layer
  → stderr
```

- **No GIL**: The atomic level check is a single `Relaxed` load
- **Simple output path**: Output goes directly to stderr; worker threads never touch Python logging
- **Python and Rust logging are separate by default**: Rust uses `tracing`, Python uses `logging`. An opt-in, GIL-free bridge (below) can forward Rust events to a host language's logging

## Opt-in bridge

The `bridge` module can capture events into a bounded queue **instead of** stderr, for a consumer thread owned by the host binding. It is off by default; while it is off, stderr output is unchanged.

```text
tracing::warn!("...")
  → AtomicLevelFilter (reads AtomicU8, ~1ns, zero GIL)
  → BridgeLayer (format message + fields, try-push into the bounded queue)
  → consumer thread → bridge::next_batch()
```

- Emitting threads never wait for queue capacity: a full queue drops the record and counts it (`bridge::dropped_total()`).
- Each `LogRecord` owns its message (formatted like the message part of the stderr line, fields as ` name=value`) and carries level, target, file, line, emission timestamp and emitting thread name. Target and file come from static callsite metadata.
- The crate stays language-neutral: it never touches Python. xbbg's Python package runs the consumer (`xbbg.enable_python_logging()`) and turns records into `logging.LogRecord`s.

```rust
use std::time::Duration;
use xbbg_log::bridge;

bridge::enable(10_000);                          // events now go to the queue, not stderr
let batch = bridge::next_batch(Duration::from_millis(100), 256);
for record in &batch.records { /* forward record.message, record.level, ... */ }
if batch.dropped > 0 { /* the queue overflowed since the previous batch */ }
if batch.closed { /* disabled and empty: stop */ }
bridge::disable();                               // stderr resumes; queued records stay readable
bridge::discard_queued();                        // a stuck consumer gave up: drop and count the rest
```

There is a single queue, so run a single consumer.

## Crate structure

```
xbbg-log/
├── Cargo.toml
└── src/
    ├── lib.rs      AtomicLevelFilter, init(), set_level(), re-exported tracing macros
    └── bridge.rs   Opt-in bounded queue: enable(), disable(), next_batch(), LogRecord
```

## Usage

### From Python

```python
import xbbg
xbbg.set_log_level("debug")   # sets AtomicU8, returns immediately
xbbg.set_log_level("warn")    # back to quiet (default)
```

### From Rust (other workspace crates)

```rust
use xbbg_log::{trace, debug, info, warn, error};

info!(worker_id = 0, "request completed");
debug!(ticker = "AAPL US Equity", "parsing response");
```

All workspace crates depend on `xbbg-log` instead of `tracing` directly, so the macro re-exports and subscriber setup are centralized.

### Developer override

Set `RUST_LOG` to a simple level to choose the initial Rust log level:

```bash
RUST_LOG=debug python my_script.py
```

Python can still change the level later via `xbbg.set_log_level()`.

### Forwarding to Python `logging`

```python
import logging
import xbbg

logging.basicConfig(level=logging.INFO)
xbbg.set_log_level("info")                 # what the native layer emits at all
xbbg.enable_python_logging(queue_size=10_000)
# ... Rust and SDK lines now arrive as records on loggers such as
# "xbbg_async.engine" and "xbbg.sdk", with their original timestamps.
xbbg.python_logging_dropped()              # records dropped because the queue was full
xbbg.disable_python_logging()              # drain (up to timeout), stderr resumes
```

## Modes

| `RUST_LOG` set? | Behaviour |
|-----------------|-----------|
| No (default) | `AtomicLevelFilter` — Python controls via `set_log_level()`, default WARN |
| Simple level | Sets the initial atomic level; Python can still update it |

## API

| Function | Description |
|----------|-------------|
| `init()` | Initialize subscriber (call once from PyO3 module init) |
| `set_level(Level)` | Set global log level (atomic store) |
| `current_level() → Level` | Get current level (atomic load) |
| `parse_level(&str) → Option<Level>` | Parse `"debug"`, `"warn"`, `"0"`–`"4"`, etc. |
| `bridge::enable(capacity) → bool` | Capture events into a bounded queue instead of stderr; `false` if already on |
| `bridge::disable()` | Stop capture (stderr resumes) and wake the consumer; queued records stay readable |
| `bridge::next_batch(timeout, max) → LogBatch` | Wait for records; the batch carries `records`, `dropped` since the last batch, and `closed` |
| `bridge::discard_queued() → usize` | Drop leftovers (counted as dropped) |
| `bridge::count_dropped(n)` | Count records the consumer took but did not deliver |
| `bridge::dropped_total() → u64` | Records dropped since process start |
| `bridge::is_enabled() → bool` | Whether capture is on |
