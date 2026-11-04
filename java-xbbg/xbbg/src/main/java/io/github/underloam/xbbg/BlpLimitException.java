package io.github.underloam.xbbg;

import java.util.Collection;

/** A Bloomberg request failed because a capacity or usage limit was reached. */
public class BlpLimitException extends BlpRequestException {
  private static final long serialVersionUID = 1L;

  /**
   * Creates an exception.
   *
   * @param message the message
   */
  public BlpLimitException(String message) {
    this(message, null, null, null, null, null, null);
  }

  /**
   * Creates an exception.
   *
   * @param message the message
   * @param service the Bloomberg service, or {@code null}
   * @param operation the request operation, or {@code null}
   * @param requestId xbbg's request label, or {@code null}
   * @param code the error code, or {@code null} for {@code LIMIT}
   * @param bloombergRequestIds Bloomberg support request ids, or {@code null}
   * @param cause the cause, or {@code null}
   */
  public BlpLimitException(
      String message,
      String service,
      String operation,
      String requestId,
      String code,
      Collection<String> bloombergRequestIds,
      Throwable cause) {
    super(message, service, operation, requestId, code, bloombergRequestIds, cause);
  }

  @Override
  public BlpErrorKind kind() {
    return BlpErrorKind.LIMIT;
  }
}
