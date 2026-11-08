package io.github.underloam.xbbg;

import java.util.Collection;
import java.util.Optional;

/** A Bloomberg request failed (also raised for {@code onDelayed = RAISE}). */
public class BlpRequestException extends BlpException {
  private static final long serialVersionUID = 1L;

  /** The Bloomberg service of the failed request, or {@code null}. */
  private final String service;

  /** The operation of the failed request, or {@code null}. */
  private final String operation;

  /** xbbg's label of the failed request, or {@code null}. */
  private final String requestId;

  /**
   * Creates an exception.
   *
   * @param message the message
   */
  public BlpRequestException(String message) {
    this(message, null, null, null, null, null, null);
  }

  /**
   * Creates an exception.
   *
   * @param message the message
   * @param service the Bloomberg service, or {@code null}
   * @param operation the request operation, or {@code null}
   * @param requestId xbbg's request label, or {@code null}
   * @param code the error code, or {@code null} for {@code REQUEST}
   * @param bloombergRequestIds Bloomberg support request ids, or {@code null}
   * @param cause the cause, or {@code null}
   */
  public BlpRequestException(
      String message,
      String service,
      String operation,
      String requestId,
      String code,
      Collection<String> bloombergRequestIds,
      Throwable cause) {
    super(message, code, bloombergRequestIds, cause);
    this.service = service;
    this.operation = operation;
    this.requestId = requestId;
  }

  @Override
  public BlpErrorKind kind() {
    return BlpErrorKind.REQUEST;
  }

  /**
   * Returns the Bloomberg service of the failed request.
   *
   * @return the service URI, or empty
   */
  public Optional<String> service() {
    return Optional.ofNullable(service);
  }

  /**
   * Returns the operation of the failed request.
   *
   * @return the operation name, or empty
   */
  public Optional<String> operation() {
    return Optional.ofNullable(operation);
  }

  /**
   * Returns xbbg's label of the failed request ({@code RequestSpec.requestId}).
   *
   * @return the label, or empty
   */
  public Optional<String> requestId() {
    return Optional.ofNullable(requestId);
  }
}
