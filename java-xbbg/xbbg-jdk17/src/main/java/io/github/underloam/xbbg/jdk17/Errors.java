package io.github.underloam.xbbg.jdk17;

import io.github.underloam.xbbg.BlpInternalException;
import io.github.underloam.xbbg.BlpLimitException;
import io.github.underloam.xbbg.BlpRequestException;
import io.github.underloam.xbbg.BlpSessionException;
import io.github.underloam.xbbg.BlpSubscriptionDataLossException;
import io.github.underloam.xbbg.BlpTimeoutException;
import io.github.underloam.xbbg.BlpValidationException;
import io.github.underloam.xbbg.BlpapiNotFoundException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/** Classifies xbbg failures as {@link ErrorKind}s. */
public final class Errors {
  private Errors() {}

  /**
   * Classifies a failure. {@link CompletionException} and {@link ExecutionException} wrappers are
   * unwrapped first, so the failure of a {@code CompletableFuture} can be passed as-is. The most
   * specific case wins: a {@link BlpLimitException} is {@link ErrorKind.Limit}, not {@link
   * ErrorKind.Request}, and a {@link CancellationException} is {@link ErrorKind.Cancelled}, not
   * {@link ErrorKind.State}.
   *
   * @param error the failure
   * @return its kind
   */
  public static ErrorKind classify(Throwable error) {
    Objects.requireNonNull(error, "error");
    Throwable cause = error;
    while ((cause instanceof CompletionException || cause instanceof ExecutionException)
        && cause.getCause() != null) {
      cause = cause.getCause();
    }
    if (cause instanceof BlpLimitException limit) {
      return new ErrorKind.Limit(limit);
    }
    if (cause instanceof BlpRequestException request) {
      return new ErrorKind.Request(request);
    }
    if (cause instanceof BlpSessionException session) {
      return new ErrorKind.Session(session);
    }
    if (cause instanceof BlpSubscriptionDataLossException dataLoss) {
      return new ErrorKind.DataLoss(dataLoss);
    }
    if (cause instanceof BlpValidationException validation) {
      return new ErrorKind.Validation(validation);
    }
    if (cause instanceof BlpTimeoutException timeout) {
      return new ErrorKind.Timeout(timeout);
    }
    if (cause instanceof BlpInternalException internal) {
      return new ErrorKind.Internal(internal);
    }
    if (cause instanceof BlpapiNotFoundException notFound) {
      return new ErrorKind.SdkNotFound(notFound);
    }
    if (cause instanceof IllegalArgumentException argument) {
      return new ErrorKind.Argument(argument);
    }
    if (cause instanceof CancellationException cancelled) {
      return new ErrorKind.Cancelled(cancelled);
    }
    if (cause instanceof IllegalStateException state) {
      return new ErrorKind.State(state);
    }
    return new ErrorKind.Other(cause);
  }
}
