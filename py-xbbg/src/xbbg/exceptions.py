"""Bloomberg API exceptions.

Canonical exception classes are defined by the Rust extension module
(`xbbg._core`) and re-exported here for a stable Python import path.

Python-only exceptions should be additive and inherit from the Rust base.
"""

from __future__ import annotations

from . import _core

# Rust base exceptions that are not extended — re-exported as-is.
BlpError = _core.BlpError
BlpSessionError = _core.BlpSessionError
BlpTimeoutError = _core.BlpTimeoutError
BlpInternalError = _core.BlpInternalError


class BlpRequestError(_core.BlpRequestError):
    """Bloomberg request-level error with extended request context attributes."""

    def __init__(
        self,
        message: str,
        *,
        service: str | None = None,
        operation: str | None = None,
        request_id: str | None = None,
        code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.service = service
        self.operation = operation
        self.request_id = request_id
        self.code = code


class BlpSubscriptionDataLossError(BlpRequestError):
    """Subscription stream became incomplete and must be resubscribed."""

    def __init__(self, message: str, *, topic: str, detail: str) -> None:
        super().__init__(message)
        self.topic = topic
        self.detail = detail


class BlpLimitError(BlpRequestError):
    """Bloomberg request limit error."""


class BlpSecurityError(BlpRequestError):
    """Bloomberg security-level error (request failed for a specific security)."""


class BlpFieldError(BlpRequestError):
    """Bloomberg field-level error (request failed for a specific field)."""


class BlpBPipeError(BlpError):
    """B-PIPE environment and entitlements required for this operation.

    Raised when attempting to use features that require a Bloomberg B-PIPE
    environment and applicable service entitlements, but only a Terminal-only
    connection is available.

    B-PIPE features include:
        - Level 2 market depth data (depth/adepth)
        - Option and futures chains (chains/achains)
    """


def _parse_validation_error(message: str) -> tuple[str | None, str | None]:
    """Extract (element, suggestion) from Rust validation error text."""
    suggestion = None
    if "(did you mean '" in message:
        start = message.find("(did you mean '") + len("(did you mean '")
        end = message.find("'?)", start)
        if end > start:
            suggestion = message[start:end]

    element = None
    if "Unknown element '" in message:
        start = message.find("Unknown element '") + len("Unknown element '")
        end = message.find("'", start)
        if end > start:
            element = message[start:end]
    elif "Invalid enum value" in message and "for '" in message:
        start = message.find("for '") + len("for '")
        end = message.find("'", start)
        if end > start:
            element = message[start:end]

    return element, suggestion


class BlpValidationError(_core.BlpValidationError):
    """Bloomberg validation error with element and suggestion metadata."""

    def __init__(
        self,
        message: str,
        *,
        element: str | None = None,
        suggestion: str | None = None,
        valid_values: list[str] | None = None,
    ) -> None:
        super().__init__(message)
        self.element = element
        self.suggestion = suggestion
        self.valid_values = valid_values

    @classmethod
    def from_rust_error(cls, message: str) -> BlpValidationError:
        """Construct a BlpValidationError by parsing metadata from a Rust error message."""
        element, suggestion = _parse_validation_error(message)
        return cls(message, element=element, suggestion=suggestion)


__all__ = [
    "BlpError",
    "BlpSessionError",
    "BlpRequestError",
    "BlpSubscriptionDataLossError",
    "BlpLimitError",
    "BlpSecurityError",
    "BlpFieldError",
    "BlpValidationError",
    "BlpTimeoutError",
    "BlpInternalError",
    "BlpBPipeError",
]
