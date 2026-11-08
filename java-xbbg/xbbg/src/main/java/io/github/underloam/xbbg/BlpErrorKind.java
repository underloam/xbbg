package io.github.underloam.xbbg;

/**
 * Category of a {@link BlpException}, as classified by the native engine.
 *
 * <p>The value is the native error kind code. Kinds without a {@code BlpException} class map to
 * standard Java exceptions: 8 to {@link IllegalArgumentException}, 9 to {@link
 * IllegalStateException} and 10 to {@link java.util.concurrent.CancellationException}.
 */
public enum BlpErrorKind {
  /** Session start, connection and worker failures ({@link BlpSessionException}). */
  SESSION(1),
  /** Bloomberg request failures ({@link BlpRequestException}). */
  REQUEST(2),
  /** Request failures caused by a capacity or usage limit ({@link BlpLimitException}). */
  LIMIT(3),
  /** Terminal subscription gaps ({@link BlpSubscriptionDataLossException}). */
  DATALOSS(4),
  /** Rejected configuration, request or subscription input ({@link BlpValidationException}). */
  VALIDATION(5),
  /** Request and identity timeouts ({@link BlpTimeoutException}). */
  TIMEOUT(6),
  /** Internal failures ({@link BlpInternalException}). */
  INTERNAL(7),
  /** No usable Bloomberg runtime library ({@link BlpapiNotFoundException}). */
  SDK_NOT_FOUND(11);

  private final int value;

  BlpErrorKind(int value) {
    this.value = value;
  }

  /**
   * Returns the native error kind code.
   *
   * @return the code used by the native library
   */
  public int value() {
    return value;
  }
}
