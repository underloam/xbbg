"""Tests for the xbbg exception hierarchy.

These tests verify that:
1. Python exceptions are properly defined
2. Rust _core exceptions are exposed and can be caught
3. Exception hierarchy allows catching by base class
4. Individual exception class attributes and behavior
5. BlpValidationError.from_rust_error() parsing
6. BlpBPipeError B-PIPE license exceptions

Tests 4-6 ported from main branch xbbg/tests/test_exceptions.py.
"""

from __future__ import annotations

import pytest


class TestPythonExceptionHierarchy:
    """Tests for the Python-defined exception classes."""

    def test_blp_error_is_base_exception(self):
        """BlpError should be the base class for all xbbg exceptions."""
        from xbbg.exceptions import BlpError

        assert issubclass(BlpError, Exception)

    def test_blp_session_error_inherits_from_blp_error(self):
        """BlpSessionError should inherit from BlpError."""
        from xbbg.exceptions import BlpError, BlpSessionError

        assert issubclass(BlpSessionError, BlpError)

    def test_blp_request_error_inherits_from_blp_error(self):
        """BlpRequestError should inherit from BlpError."""
        from xbbg.exceptions import BlpError, BlpRequestError

        assert issubclass(BlpRequestError, BlpError)

    def test_subscription_data_loss_inherits_from_request_error(self):
        from xbbg.exceptions import BlpRequestError, BlpSubscriptionDataLossError

        assert issubclass(BlpSubscriptionDataLossError, BlpRequestError)

    def test_subscription_data_loss_is_exported_at_package_root(self):
        import xbbg
        from xbbg.exceptions import BlpSubscriptionDataLossError

        assert xbbg.BlpSubscriptionDataLossError is BlpSubscriptionDataLossError

    def test_blp_limit_error_inherits_from_blp_request_error(self):
        """BlpLimitError should inherit from BlpRequestError."""
        from xbbg.exceptions import BlpLimitError, BlpRequestError

        assert issubclass(BlpLimitError, BlpRequestError)

    def test_blp_security_error_inherits_from_blp_request_error(self):
        """BlpSecurityError should inherit from BlpRequestError."""
        from xbbg.exceptions import BlpRequestError, BlpSecurityError

        assert issubclass(BlpSecurityError, BlpRequestError)

    def test_blp_field_error_inherits_from_blp_request_error(self):
        """BlpFieldError should inherit from BlpRequestError."""
        from xbbg.exceptions import BlpFieldError, BlpRequestError

        assert issubclass(BlpFieldError, BlpRequestError)

    def test_blp_validation_error_inherits_from_blp_error(self):
        """BlpValidationError should inherit from BlpError."""
        from xbbg.exceptions import BlpError, BlpValidationError

        assert issubclass(BlpValidationError, BlpError)

    def test_blp_timeout_error_inherits_from_blp_error(self):
        """BlpTimeoutError should inherit from BlpError."""
        from xbbg.exceptions import BlpError, BlpTimeoutError

        assert issubclass(BlpTimeoutError, BlpError)

    def test_blp_internal_error_inherits_from_blp_error(self):
        """BlpInternalError should inherit from BlpError."""
        from xbbg.exceptions import BlpError, BlpInternalError

        assert issubclass(BlpInternalError, BlpError)

    def test_blp_request_error_has_context_attributes(self):
        """BlpRequestError should have service/operation/request_id/code attributes."""
        from xbbg.exceptions import BlpRequestError

        err = BlpRequestError(
            "test error",
            service="//blp/refdata",
            operation="ReferenceDataRequest",
            request_id="req-123",
            code=42,
        )

        assert str(err) == "test error"
        assert err.service == "//blp/refdata"
        assert err.operation == "ReferenceDataRequest"
        assert err.request_id == "req-123"
        assert err.code == 42

    def test_subscription_data_loss_preserves_gap_context(self):
        from xbbg.exceptions import BlpSubscriptionDataLossError

        err = BlpSubscriptionDataLossError(
            "subscription gap",
            topic="IBM US Equity",
            detail="consumer queue overflow",
        )

        assert str(err) == "subscription gap"
        assert err.topic == "IBM US Equity"
        assert err.detail == "consumer queue overflow"

    def test_catching_by_base_class(self):
        """Should be able to catch any xbbg exception with BlpError."""
        from xbbg.exceptions import (
            BlpError,
            BlpFieldError,
            BlpInternalError,
            BlpLimitError,
            BlpRequestError,
            BlpSecurityError,
            BlpSessionError,
            BlpSubscriptionDataLossError,
            BlpTimeoutError,
            BlpValidationError,
        )

        exceptions = [
            BlpSessionError("session error"),
            BlpRequestError("request error"),
            BlpSubscriptionDataLossError(
                "subscription gap",
                topic="IBM US Equity",
                detail="consumer queue overflow",
            ),
            BlpSecurityError("security error"),
            BlpFieldError("field error"),
            BlpLimitError("limit error"),
            BlpValidationError("validation error"),
            BlpTimeoutError("timeout error"),
            BlpInternalError("internal error"),
        ]

        for exc in exceptions:
            try:
                raise exc
            except BlpError as e:
                # Should catch all of them
                assert str(e) in str(exc)
            else:
                pytest.fail(f"Failed to catch {type(exc).__name__} with BlpError")


class TestRustCoreExceptions:
    """Tests for the Rust _core exception classes exposed to Python."""

    def test_core_exceptions_exist(self):
        """Rust _core module should expose exception classes."""
        import xbbg

        # Access _core through xbbg to trigger DLL path setup
        _core = xbbg._core

        # These exception classes should be exposed by the Rust module
        assert hasattr(_core, "BlpError")
        assert hasattr(_core, "BlpSessionError")
        assert hasattr(_core, "BlpRequestError")
        assert hasattr(_core, "BlpSecurityError")
        assert hasattr(_core, "BlpFieldError")
        assert hasattr(_core, "BlpSubscriptionDataLossError")
        assert hasattr(_core, "BlpValidationError")
        assert hasattr(_core, "BlpTimeoutError")
        assert hasattr(_core, "BlpInternalError")

    def test_core_exceptions_are_exception_types(self):
        """Rust _core exceptions should be exception types."""
        import xbbg

        _core = xbbg._core

        # All should be exception classes (types that inherit from BaseException)
        assert isinstance(_core.BlpError, type)
        assert issubclass(_core.BlpError, BaseException)
        assert issubclass(_core.BlpSessionError, BaseException)
        assert issubclass(_core.BlpRequestError, BaseException)
        assert issubclass(_core.BlpSubscriptionDataLossError, BaseException)
        assert issubclass(_core.BlpValidationError, BaseException)
        assert issubclass(_core.BlpTimeoutError, BaseException)
        assert issubclass(_core.BlpInternalError, BaseException)

    def test_core_session_error_inherits_from_blp_error(self):
        """Rust BlpSessionError should inherit from BlpError."""
        import xbbg

        _core = xbbg._core

        assert issubclass(_core.BlpSessionError, _core.BlpError)

    def test_core_request_error_inherits_from_blp_error(self):
        """Rust BlpRequestError should inherit from BlpError."""
        import xbbg

        _core = xbbg._core

        assert issubclass(_core.BlpRequestError, _core.BlpError)

    def test_core_subscription_data_loss_inherits_from_request_error(self):
        import xbbg

        _core = xbbg._core

        assert issubclass(_core.BlpSubscriptionDataLossError, _core.BlpRequestError)

    def test_core_validation_error_inherits_from_blp_error(self):
        """Rust BlpValidationError should inherit from BlpError."""
        import xbbg

        _core = xbbg._core

        assert issubclass(_core.BlpValidationError, _core.BlpError)

    def test_core_timeout_error_inherits_from_blp_error(self):
        """Rust BlpTimeoutError should inherit from BlpError."""
        import xbbg

        _core = xbbg._core

        assert issubclass(_core.BlpTimeoutError, _core.BlpError)

    def test_core_internal_error_inherits_from_blp_error(self):
        """Rust BlpInternalError should inherit from BlpError."""
        import xbbg

        _core = xbbg._core

        assert issubclass(_core.BlpInternalError, _core.BlpError)

    def test_core_security_error_inherits_from_request_error(self):
        """Rust BlpSecurityError should inherit from BlpRequestError."""
        import xbbg

        _core = xbbg._core

        assert issubclass(_core.BlpSecurityError, _core.BlpRequestError)

    def test_core_field_error_inherits_from_request_error(self):
        """Rust BlpFieldError should inherit from BlpRequestError."""
        import xbbg

        _core = xbbg._core

        assert issubclass(_core.BlpFieldError, _core.BlpRequestError)

    def test_core_limit_error_inherits_from_request_error(self):
        """Rust BlpLimitError should inherit from BlpRequestError."""
        import xbbg

        _core = xbbg._core

        assert issubclass(_core.BlpLimitError, _core.BlpRequestError)

    def test_core_exceptions_can_be_raised_and_caught(self):
        """Rust _core exceptions should be raisable and catchable."""
        import xbbg

        _core = xbbg._core

        # Test raising and catching each exception type
        with pytest.raises(_core.BlpSessionError):
            raise _core.BlpSessionError("test session error")

        with pytest.raises(_core.BlpRequestError):
            raise _core.BlpRequestError("test request error")

        with pytest.raises(_core.BlpValidationError):
            raise _core.BlpValidationError("test validation error")

        with pytest.raises(_core.BlpTimeoutError):
            raise _core.BlpTimeoutError("test timeout error")

        with pytest.raises(_core.BlpInternalError):
            raise _core.BlpInternalError("test internal error")

    def test_core_exceptions_catchable_by_base(self):
        """Rust _core exceptions should be catchable by their base class."""
        import xbbg

        _core = xbbg._core

        # BlpSessionError should be catchable by BlpError
        with pytest.raises(_core.BlpError):
            raise _core.BlpSessionError("session error")

        # BlpRequestError should be catchable by BlpError
        with pytest.raises(_core.BlpError):
            raise _core.BlpRequestError("request error")

        # BlpSecurityError should be catchable by BlpRequestError
        with pytest.raises(_core.BlpRequestError):
            raise _core.BlpSecurityError("security error")

        # BlpFieldError should be catchable by BlpRequestError
        with pytest.raises(_core.BlpRequestError):
            raise _core.BlpFieldError("field error")


class TestValidationErrorsFromPython:
    """Tests for validation errors raised from the Python layer."""

    def test_missing_securities_raises_validation_error(self):
        """RequestParams validation should raise BlpValidationError for missing securities."""
        from xbbg.exceptions import BlpValidationError
        from xbbg.services import Operation, RequestParams, Service

        params = RequestParams(
            service=Service.REFDATA,
            operation=Operation.REFERENCE_DATA,
            securities=None,  # Missing!
            fields=["PX_LAST"],
        )

        with pytest.raises(BlpValidationError, match="securities is required"):
            params.validate()

    def test_missing_fields_raises_validation_error(self):
        """RequestParams validation should raise BlpValidationError for missing fields."""
        from xbbg.exceptions import BlpValidationError
        from xbbg.services import Operation, RequestParams, Service

        params = RequestParams(
            service=Service.REFDATA,
            operation=Operation.REFERENCE_DATA,
            securities=["AAPL US Equity"],
            fields=None,  # Missing!
        )

        with pytest.raises(BlpValidationError, match="fields is required"):
            params.validate()

    def test_missing_dates_for_historical_raises_validation_error(self):
        """RequestParams validation should raise BlpValidationError for missing dates."""
        from xbbg.exceptions import BlpValidationError
        from xbbg.services import Operation, RequestParams, Service

        params = RequestParams(
            service=Service.REFDATA,
            operation=Operation.HISTORICAL_DATA,
            securities=["AAPL US Equity"],
            fields=["PX_LAST"],
            start_date=None,  # Missing!
            end_date="20241201",
        )

        with pytest.raises(BlpValidationError, match="start_date is required"):
            params.validate()

    def test_missing_security_for_intraday_raises_validation_error(self):
        """RequestParams validation should raise BlpValidationError for intraday requests."""
        from xbbg.exceptions import BlpValidationError
        from xbbg.services import Operation, RequestParams, Service

        params = RequestParams(
            service=Service.REFDATA,
            operation=Operation.INTRADAY_BAR,
            security=None,  # Missing! (intraday uses 'security' not 'securities')
            event_type="TRADE",
            interval=1,
            start_datetime="2024-12-01T09:30:00",
            end_datetime="2024-12-01T16:00:00",
        )

        with pytest.raises(BlpValidationError, match="security is required"):
            params.validate()


class TestBlpErrorIndividual:
    """Tests for BlpError base exception class (ported from main branch)."""

    def test_blp_error_can_be_raised(self):
        """BlpError can be raised and caught."""
        from xbbg.exceptions import BlpError

        with pytest.raises(BlpError):
            raise BlpError("Test error")

    def test_blp_error_message(self):
        """BlpError stores message correctly."""
        from xbbg.exceptions import BlpError

        error = BlpError("Test message")
        assert str(error) == "Test message"


class TestBlpSessionErrorIndividual:
    """Tests for BlpSessionError exception class (ported from main branch)."""

    def test_can_be_raised(self):
        """BlpSessionError can be raised and caught."""
        from xbbg.exceptions import BlpSessionError

        with pytest.raises(BlpSessionError):
            raise BlpSessionError("Session failed to start")

    def test_message(self):
        """BlpSessionError stores message correctly."""
        from xbbg.exceptions import BlpSessionError

        error = BlpSessionError("Connection refused")
        assert str(error) == "Connection refused"


class TestBlpRequestErrorIndividual:
    """Tests for BlpRequestError with partial/default attributes (ported from main branch)."""

    def test_basic_no_attributes(self):
        """BlpRequestError with just a message should have None attributes."""
        from xbbg.exceptions import BlpRequestError

        error = BlpRequestError("Request failed")
        assert str(error) == "Request failed"
        assert error.service is None
        assert error.operation is None
        assert error.request_id is None
        assert error.code is None

    def test_partial_attributes(self):
        """BlpRequestError with some optional attributes."""
        from xbbg.exceptions import BlpRequestError

        error = BlpRequestError(
            "Request failed",
            service="//blp/refdata",
            code=404,
        )
        assert error.service == "//blp/refdata"
        assert error.operation is None
        assert error.request_id is None
        assert error.code == 404


class TestBlpSecurityErrorIndividual:
    """Tests for BlpSecurityError (ported from main branch)."""

    def test_inherits_from_blp_request_error(self):
        """BlpSecurityError should inherit from BlpRequestError."""
        from xbbg.exceptions import BlpRequestError, BlpSecurityError

        assert issubclass(BlpSecurityError, BlpRequestError)

    def test_with_request_attributes(self):
        """BlpSecurityError should support request attributes."""
        from xbbg.exceptions import BlpSecurityError

        error = BlpSecurityError(
            "Invalid security: INVALID US Equity",
            service="//blp/refdata",
            operation="ReferenceDataRequest",
        )
        assert "Invalid security" in str(error)
        assert error.service == "//blp/refdata"


class TestBlpFieldErrorIndividual:
    """Tests for BlpFieldError (ported from main branch)."""

    def test_with_request_attributes(self):
        """BlpFieldError should support request attributes."""
        from xbbg.exceptions import BlpFieldError

        error = BlpFieldError(
            "Invalid field: INVALID_FIELD",
            service="//blp/refdata",
            code=100,
        )
        assert "Invalid field" in str(error)
        assert error.code == 100


class TestBlpValidationErrorAttributes:
    """Tests for BlpValidationError attributes (ported from main branch)."""

    def test_basic_no_attributes(self):
        """BlpValidationError with just a message should have None attributes."""
        from xbbg.exceptions import BlpValidationError

        error = BlpValidationError("Validation failed")
        assert str(error) == "Validation failed"
        assert error.element is None
        assert error.suggestion is None
        assert error.valid_values is None

    def test_with_all_attributes(self):
        """BlpValidationError with all optional attributes."""
        from xbbg.exceptions import BlpValidationError

        error = BlpValidationError(
            "Invalid enum value",
            element="periodicitySelection",
            suggestion="DAILY",
            valid_values=["DAILY", "WEEKLY", "MONTHLY"],
        )
        assert error.element == "periodicitySelection"
        assert error.suggestion == "DAILY"
        assert error.valid_values == ["DAILY", "WEEKLY", "MONTHLY"]


class TestBlpValidationErrorFromRustError:
    """Tests for BlpValidationError.from_rust_error() parsing.

    Ported from main branch's TestBlpValidationError.test_from_error_message_*
    tests. The current branch uses from_rust_error() instead of from_error_message().
    """

    def test_with_suggestion(self):
        """from_rust_error() should extract suggestion from 'did you mean' pattern."""
        from xbbg.exceptions import BlpValidationError

        message = "Unknown element 'periodictySelection' (did you mean 'periodicitySelection'?)"
        error = BlpValidationError.from_rust_error(message)
        assert error.suggestion == "periodicitySelection"
        assert error.element == "periodictySelection"

    def test_with_element_only(self):
        """from_rust_error() should extract element without suggestion."""
        from xbbg.exceptions import BlpValidationError

        message = "Unknown element 'invalidField'"
        error = BlpValidationError.from_rust_error(message)
        assert error.element == "invalidField"
        assert error.suggestion is None

    def test_with_invalid_enum(self):
        """from_rust_error() should extract element from invalid enum message."""
        from xbbg.exceptions import BlpValidationError

        message = "Invalid enum value 'DAYLY' for 'periodicitySelection'"
        error = BlpValidationError.from_rust_error(message)
        assert error.element == "periodicitySelection"
        assert error.suggestion is None

    def test_no_pattern_match(self):
        """from_rust_error() with unrecognized message should set None attributes."""
        from xbbg.exceptions import BlpValidationError

        message = "Some other validation error"
        error = BlpValidationError.from_rust_error(message)
        assert str(error) == message
        assert error.element is None
        assert error.suggestion is None

    def test_preserves_full_message(self):
        """from_rust_error() should preserve the full original message."""
        from xbbg.exceptions import BlpValidationError

        message = "Unknown element 'foo' (did you mean 'bar'?)"
        error = BlpValidationError.from_rust_error(message)
        assert str(error) == message


class TestBlpBPipeError:
    """Tests for BlpBPipeError B-PIPE license exception."""

    def test_inherits_from_blp_error(self):
        """BlpBPipeError should inherit from BlpError."""
        from xbbg.exceptions import BlpBPipeError, BlpError

        assert issubclass(BlpBPipeError, BlpError)

    def test_can_be_raised(self):
        """BlpBPipeError can be raised and caught."""
        from xbbg.exceptions import BlpBPipeError

        with pytest.raises(BlpBPipeError):
            raise BlpBPipeError("B-PIPE license required")

    def test_catchable_by_base(self):
        """BlpBPipeError should be catchable by BlpError."""
        from xbbg.exceptions import BlpBPipeError, BlpError

        with pytest.raises(BlpError):
            raise BlpBPipeError("B-PIPE license required")

    def test_message(self):
        """BlpBPipeError stores message correctly."""
        from xbbg.exceptions import BlpBPipeError

        error = BlpBPipeError("B-PIPE license required for depth data")
        assert "B-PIPE" in str(error)


class TestExceptionHierarchyComplete:
    """Complete exception hierarchy tests (ported from main branch)."""

    def test_all_exceptions_inherit_from_blp_error(self):
        """All custom exceptions should inherit from BlpError."""
        from xbbg.exceptions import (
            BlpBPipeError,
            BlpError,
            BlpFieldError,
            BlpInternalError,
            BlpLimitError,
            BlpRequestError,
            BlpSecurityError,
            BlpSessionError,
            BlpSubscriptionDataLossError,
            BlpTimeoutError,
            BlpValidationError,
        )

        exceptions = [
            BlpSessionError,
            BlpRequestError,
            BlpLimitError,
            BlpSecurityError,
            BlpFieldError,
            BlpSubscriptionDataLossError,
            BlpValidationError,
            BlpTimeoutError,
            BlpInternalError,
            BlpBPipeError,
        ]
        for exc in exceptions:
            assert issubclass(exc, BlpError), f"{exc.__name__} should inherit from BlpError"

    def test_request_family_errors_inherit_from_request_error(self):
        """Request-family exceptions should inherit from BlpRequestError."""
        from xbbg.exceptions import (
            BlpFieldError,
            BlpLimitError,
            BlpRequestError,
            BlpSecurityError,
            BlpSubscriptionDataLossError,
        )

        assert issubclass(BlpLimitError, BlpRequestError)
        assert issubclass(BlpSecurityError, BlpRequestError)
        assert issubclass(BlpFieldError, BlpRequestError)
        assert issubclass(BlpSubscriptionDataLossError, BlpRequestError)

    def test_exception_chain_catching(self):
        """Exception hierarchy should allow proper chain catching."""
        from xbbg.exceptions import BlpError, BlpRequestError, BlpSecurityError

        # BlpSecurityError caught by BlpRequestError
        try:
            raise BlpSecurityError("Invalid security")
        except BlpRequestError as e:
            assert "Invalid security" in str(e)

        # BlpRequestError caught by BlpError
        try:
            raise BlpRequestError("Request failed")
        except BlpError as e:
            assert "Request failed" in str(e)

    def test_specific_exception_not_caught_by_sibling(self):
        """Sibling exceptions should not catch each other."""
        from xbbg.exceptions import BlpRequestError, BlpSessionError

        with pytest.raises(BlpSessionError):
            try:
                raise BlpSessionError("Session error")
            except BlpRequestError:
                pytest.fail("BlpSessionError should not be caught by BlpRequestError")
