package io.github.underloam.xbbg;

import java.util.Collection;

/** An internal failure: a closed channel, a native panic, or a broken native contract. */
public class BlpInternalException extends BlpException {
  private static final long serialVersionUID = 1L;

  /**
   * Creates an exception.
   *
   * @param message the message
   */
  public BlpInternalException(String message) {
    this(message, null, null, null);
  }

  /**
   * Creates an exception.
   *
   * @param message the message
   * @param cause the cause, or {@code null}
   */
  public BlpInternalException(String message, Throwable cause) {
    this(message, null, null, cause);
  }

  /**
   * Creates an exception.
   *
   * @param message the message
   * @param code the error code, or {@code null} for {@code INTERNAL}
   * @param bloombergRequestIds Bloomberg support request ids, or {@code null}
   * @param cause the cause, or {@code null}
   */
  public BlpInternalException(
      String message, String code, Collection<String> bloombergRequestIds, Throwable cause) {
    super(message, code, bloombergRequestIds, cause);
  }

  @Override
  public BlpErrorKind kind() {
    return BlpErrorKind.INTERNAL;
  }
}
