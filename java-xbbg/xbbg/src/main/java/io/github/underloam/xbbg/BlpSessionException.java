package io.github.underloam.xbbg;

import java.util.Collection;

/** A Bloomberg session could not be started, was lost, or has no healthy worker. */
public class BlpSessionException extends BlpException {
  private static final long serialVersionUID = 1L;

  /**
   * Creates an exception.
   *
   * @param message the message
   */
  public BlpSessionException(String message) {
    this(message, null, null, null);
  }

  /**
   * Creates an exception.
   *
   * @param message the message
   * @param code the error code, or {@code null} for {@code SESSION}
   * @param bloombergRequestIds Bloomberg support request ids, or {@code null}
   * @param cause the cause, or {@code null}
   */
  public BlpSessionException(
      String message, String code, Collection<String> bloombergRequestIds, Throwable cause) {
    super(message, code, bloombergRequestIds, cause);
  }

  @Override
  public BlpErrorKind kind() {
    return BlpErrorKind.SESSION;
  }
}
