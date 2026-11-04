package io.github.underloam.xbbg;

/**
 * No usable Bloomberg runtime library (blpapi) was found or could be provisioned. The message lists
 * every way to supply one.
 */
public class BlpapiNotFoundException extends BlpException {
  private static final long serialVersionUID = 1L;

  /**
   * Creates an exception.
   *
   * @param message the message
   */
  public BlpapiNotFoundException(String message) {
    this(message, null, null);
  }

  /**
   * Creates an exception.
   *
   * @param message the message
   * @param code the error code, or {@code null} for {@code SDK_NOT_FOUND}
   * @param cause the cause, or {@code null}
   */
  public BlpapiNotFoundException(String message, String code, Throwable cause) {
    super(message, code, null, cause);
  }

  @Override
  public BlpErrorKind kind() {
    return BlpErrorKind.SDK_NOT_FOUND;
  }
}
