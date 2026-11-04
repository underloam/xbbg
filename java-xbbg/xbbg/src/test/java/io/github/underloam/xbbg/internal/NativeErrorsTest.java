package io.github.underloam.xbbg.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.BlpErrorKind;
import io.github.underloam.xbbg.BlpException;
import io.github.underloam.xbbg.BlpInternalException;
import io.github.underloam.xbbg.BlpLimitException;
import io.github.underloam.xbbg.BlpRequestException;
import io.github.underloam.xbbg.BlpSessionException;
import io.github.underloam.xbbg.BlpSubscriptionDataLossException;
import io.github.underloam.xbbg.BlpTimeoutException;
import io.github.underloam.xbbg.BlpValidationException;
import io.github.underloam.xbbg.BlpapiNotFoundException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;

class NativeErrorsTest {
  private static final String FULL_CONTEXT =
      "{\"service\":\"//blp/refdata\",\"operation\":\"ReferenceDataRequest\",\"requestId\":\"r1\","
          + "\"bloombergRequestIds\":[\"b1\"],\"topic\":\"IBM US Equity\",\"detail\":\"gap\","
          + "\"element\":\"fields\",\"suggestion\":\"PX_LAST\"}";

  @Test
  void mapsEveryKindToItsClass() {
    Class<?>[] expected = {
      BlpSessionException.class,
      BlpRequestException.class,
      BlpLimitException.class,
      BlpSubscriptionDataLossException.class,
      BlpValidationException.class,
      BlpTimeoutException.class,
      BlpInternalException.class,
      IllegalArgumentException.class,
      IllegalStateException.class,
      CancellationException.class,
      BlpapiNotFoundException.class
    };
    BlpErrorKind[] kinds = {
      BlpErrorKind.SESSION,
      BlpErrorKind.REQUEST,
      BlpErrorKind.LIMIT,
      BlpErrorKind.DATALOSS,
      BlpErrorKind.VALIDATION,
      BlpErrorKind.TIMEOUT,
      BlpErrorKind.INTERNAL,
      null,
      null,
      null,
      BlpErrorKind.SDK_NOT_FOUND
    };
    for (int kind = 1; kind <= 11; kind++) {
      Throwable error = NativeBridge.error(kind, "message " + kind, null);
      assertEquals(expected[kind - 1], error.getClass(), "kind " + kind);
      assertEquals("message " + kind, error.getMessage());
      if (kinds[kind - 1] != null) {
        BlpException blp = (BlpException) error;
        assertEquals(kinds[kind - 1], blp.kind());
        assertEquals(kinds[kind - 1].value(), kind);
        assertEquals(kinds[kind - 1].name(), blp.code());
        assertTrue(blp.bloombergRequestIds().isEmpty());
        assertFalse(blp.bloombergRequestId().isPresent());
      }
    }
  }

  @Test
  void unknownKindsBecomeInternalErrors() {
    Throwable error = NativeBridge.error(42, "boom", null);
    assertInstanceOf(BlpInternalException.class, error);
    assertEquals("native error of unknown kind 42: boom", error.getMessage());
    assertEquals("", NativeBridge.error(7, null, null).getMessage());
  }

  @Test
  void contextFillsTheStructuredFields() {
    BlpRequestException request =
        (BlpRequestException) NativeBridge.error(2, "Request failed", FULL_CONTEXT);
    assertEquals("//blp/refdata", request.service().get());
    assertEquals("ReferenceDataRequest", request.operation().get());
    assertEquals("r1", request.requestId().get());
    assertEquals("b1", request.bloombergRequestId().get());
    assertEquals("REQUEST", request.code());

    BlpLimitException limit = (BlpLimitException) NativeBridge.error(3, "limit", FULL_CONTEXT);
    assertEquals(BlpErrorKind.LIMIT, limit.kind());
    assertEquals("r1", limit.requestId().get());

    BlpSubscriptionDataLossException dataLoss =
        (BlpSubscriptionDataLossException) NativeBridge.error(4, "lost", FULL_CONTEXT);
    assertEquals("IBM US Equity", dataLoss.topic().get());
    assertEquals("gap", dataLoss.detail().get());
    assertEquals("DATALOSS", dataLoss.code());

    BlpValidationException validation =
        (BlpValidationException) NativeBridge.error(5, "bad", FULL_CONTEXT);
    assertEquals("fields", validation.element().get());
    assertEquals("PX_LAST", validation.suggestion().get());

    BlpTimeoutException timeout = (BlpTimeoutException) NativeBridge.error(6, "slow", FULL_CONTEXT);
    assertEquals("r1", timeout.requestId().get());
  }

  @Test
  void contextCodeOverridesTheKindName() {
    BlpException error =
        (BlpException) NativeBridge.error(7, "download failed", "{\"code\":\"SDK_NETWORK\"}");
    assertEquals("SDK_NETWORK", error.code());
    BlpException notFound =
        (BlpException) NativeBridge.error(11, "offline", "{\"code\":\"SDK_OFFLINE\"}");
    assertEquals("SDK_OFFLINE", notFound.code());
    assertEquals(BlpErrorKind.SDK_NOT_FOUND, notFound.kind());
  }

  @Test
  void multipleRequestIdsHaveNoSingleId() {
    BlpSessionException error =
        (BlpSessionException)
            NativeBridge.error(1, "down", "{\"bloombergRequestIds\":[\"a\",\"b\"]}");
    assertEquals(Arrays.asList("a", "b"), error.bloombergRequestIds());
    assertFalse(error.bloombergRequestId().isPresent());
  }

  @Test
  void malformedContextKeepsTheErrorAndRecordsTheProblem() {
    Throwable error = NativeBridge.error(2, "Request failed", "{not json");
    assertInstanceOf(BlpRequestException.class, error);
    assertEquals("Request failed", error.getMessage());
    assertEquals(1, error.getSuppressed().length);
    assertInstanceOf(BlpInternalException.class, error.getSuppressed()[0]);

    Throwable wrongShape = NativeBridge.error(5, "bad", "{\"element\":1}");
    assertFalse(((BlpValidationException) wrongShape).element().isPresent());
    assertEquals(1, wrongShape.getSuppressed().length);
  }

  @Test
  void publicConstructorsKeepTheirFields() {
    BlpValidationException validation = new BlpValidationException("rows=false", "rows");
    assertEquals("rows", validation.element().get());
    assertEquals("VALIDATION", validation.code());
    BlpRequestException request =
        new BlpRequestException(
            "failed", "svc", "op", "id", "CUSTOM", Collections.singletonList("x"), null);
    assertEquals("CUSTOM", request.code());
    assertEquals("x", request.bloombergRequestId().get());
    assertEquals(
        BlpErrorKind.INTERNAL, new BlpInternalException("x", new RuntimeException()).kind());
  }
}
