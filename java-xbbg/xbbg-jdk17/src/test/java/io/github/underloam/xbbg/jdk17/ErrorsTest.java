package io.github.underloam.xbbg.jdk17;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

class ErrorsTest {
  @Test
  void mapsEveryXbbgExceptionToItsCase() {
    assertCase(ErrorKind.Session.class, new BlpSessionException("down"));
    assertCase(ErrorKind.Request.class, new BlpRequestException("bad security"));
    assertCase(ErrorKind.Limit.class, new BlpLimitException("daily limit"));
    assertCase(ErrorKind.DataLoss.class, new BlpSubscriptionDataLossException("gap"));
    assertCase(ErrorKind.Validation.class, new BlpValidationException("bad field", "fields"));
    assertCase(ErrorKind.Timeout.class, new BlpTimeoutException("slow"));
    assertCase(ErrorKind.Internal.class, new BlpInternalException("panic"));
    assertCase(ErrorKind.SdkNotFound.class, new BlpapiNotFoundException("no blpapi"));
    assertCase(ErrorKind.Argument.class, new IllegalArgumentException("unknown field 'x'"));
    assertCase(ErrorKind.State.class, new IllegalStateException("engine is closed"));
    assertCase(ErrorKind.Cancelled.class, new CancellationException("cancelled"));
    assertCase(ErrorKind.Other.class, new IOException("disk"));
  }

  @Test
  void mostSpecificSubclassWins() {
    // BlpLimitException extends BlpRequestException; CancellationException extends
    // IllegalStateException.
    assertEquals(ErrorKind.Limit.class, Errors.classify(new BlpLimitException("limit")).getClass());
    assertEquals(
        ErrorKind.Cancelled.class, Errors.classify(new CancellationException()).getClass());
  }

  @Test
  void unwrapsFutureFailures() throws InterruptedException {
    BlpTimeoutException timeout = new BlpTimeoutException("slow", "req-1", null, null, null);
    CompletableFuture<Object> failed = CompletableFuture.failedFuture(timeout);
    ExecutionException execution = assertThrows(ExecutionException.class, failed::get);
    CompletionException completion =
        assertThrows(CompletionException.class, () -> failed.thenApply(x -> x).join());
    for (Throwable wrapped :
        new Throwable[] {execution, completion, new CompletionException(execution)}) {
      ErrorKind kind = Errors.classify(wrapped);
      ErrorKind.Timeout typed = (ErrorKind.Timeout) kind;
      assertSame(timeout, typed.error());
      assertEquals("req-1", typed.error().requestId().orElseThrow());
    }
    // a wrapper without a cause is classified as itself
    CompletionException bare = new CompletionException("no cause", null);
    assertSame(bare, Errors.classify(bare).error());
  }

  @Test
  void foreignBlpSubclassesAreOther() {
    BlpException custom =
        new BlpException("custom", null, null, null) {
          private static final long serialVersionUID = 1L;

          @Override
          public BlpErrorKind kind() {
            return BlpErrorKind.REQUEST;
          }
        };
    assertCase(ErrorKind.Other.class, custom);
  }

  @Test
  void rejectsNull() {
    assertThrows(NullPointerException.class, () -> Errors.classify(null));
  }

  private static void assertCase(Class<? extends ErrorKind> expected, Throwable error) {
    ErrorKind kind = Errors.classify(error);
    assertEquals(expected, kind.getClass(), () -> error.toString());
    assertSame(error, kind.error());
  }
}
