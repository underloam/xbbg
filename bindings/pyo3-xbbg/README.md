# pyo3-xbbg

PyO3 bindings for the xbbg Bloomberg engine.

This crate provides Python bindings via PyO3, exposing the Rust engine to Python as the `xbbg._core` module.

## Features

- Async API (abdp, abdh, abds, abdib, abdtick)
- Zero-copy Arrow data transfer via PyArrow
- GIL released during Bloomberg SDK operations
- Shared subscriptions with optional image-only `rows=False` delivery, field-error
  policies, and `zero_as_null` masking in `latest()` snapshots
- Auction field groups and a separate `ext_auction_zero_price_fields()` sentinel list
- Typed engine exceptions preserved by every recipe

Image-only subscriptions expose `latest()` and reject both Arrow and dict iteration.
Subscription status history and pending warnings remain readable after unsubscribe;
`latest()` on a closed subscription raises rather than fabricating an empty batch.
`delivers_rows` reflects the handle's immutable delivery mode.

The optional `session_wait_ms` subscription setting bounds pool-session acquisition;
`None` preserves unbounded waiting. Existing-feed joins never need a session claim.
A claim deadline raises `BlpValidationError` with a message beginning
`Configuration error: session_wait:` before mutating subscription state.
Both native subscribe entry points accept `rows`, `on_field_error`, `zero_as_null`,
and `session_wait_ms`; invalid field-error policies raise `ValueError`.
