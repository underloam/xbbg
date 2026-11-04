package io.github.underloam.xbbg;

import java.util.Collection;
import java.util.Optional;

/** A request or identity operation timed out ({@code EngineConfig.requestTimeoutMs}). */
public class BlpTimeoutException extends BlpException {
  private static final long serialVersionUID = 1L;

  /** xbbg's label of the request that timed out, or {@code null}. */
  private final String requestId;

  /**
   * Creates an exception.
   *
   * @param message the message
   */
  public BlpTimeoutException(String message) {
    this(message, null, null, null, null);
  }

  /**
   * Creates an exception.
   *
   * @param message the message
   * @param requestId xbbg's request label, or {@code null}
   * @param code the error code, or {@code null} for {@code TIMEOUT}
   * @param bloombergRequestIds Bloomberg support request ids, or {@code null}
   * @param cause the cause, or {@code null}
   */
  public BlpTimeoutException(
      String message,
      String requestId,
      String code,
      Collection<String> bloombergRequestIds,
      Throwable cause) {
    super(message, code, bloombergRequestIds, cause);
    this.requestId = requestId;
  }

  @Override
  public BlpErrorKind kind() {
    return BlpErrorKind.TIMEOUT;
  }

  /**
   * Returns xbbg's label of the timed-out request.
   *
   * @return the label, or empty
   */
  public Optional<String> requestId() {
    return Optional.ofNullable(requestId);
  }
}
