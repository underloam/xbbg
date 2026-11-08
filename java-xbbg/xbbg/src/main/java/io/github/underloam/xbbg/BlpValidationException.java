package io.github.underloam.xbbg;

import java.util.Collection;
import java.util.Optional;

/** Configuration, request or subscription input was rejected before or by the engine. */
public class BlpValidationException extends BlpException {
  private static final long serialVersionUID = 1L;

  /** The rejected option or argument, or {@code null}. */
  private final String element;

  /** A suggested replacement value, or {@code null}. */
  private final String suggestion;

  /**
   * Creates an exception.
   *
   * @param message the message
   */
  public BlpValidationException(String message) {
    this(message, null, null, null, null, null);
  }

  /**
   * Creates an exception naming the rejected option or argument.
   *
   * @param message the message
   * @param element the rejected option or argument, or {@code null}
   */
  public BlpValidationException(String message, String element) {
    this(message, element, null, null, null, null);
  }

  /**
   * Creates an exception.
   *
   * @param message the message
   * @param element the rejected option or argument, or {@code null}
   * @param suggestion a suggested replacement value, or {@code null}
   * @param code the error code, or {@code null} for {@code VALIDATION}
   * @param bloombergRequestIds Bloomberg support request ids, or {@code null}
   * @param cause the cause, or {@code null}
   */
  public BlpValidationException(
      String message,
      String element,
      String suggestion,
      String code,
      Collection<String> bloombergRequestIds,
      Throwable cause) {
    super(message, code, bloombergRequestIds, cause);
    this.element = element;
    this.suggestion = suggestion;
  }

  @Override
  public BlpErrorKind kind() {
    return BlpErrorKind.VALIDATION;
  }

  /**
   * Returns the rejected option or argument name.
   *
   * @return the element, or empty
   */
  public Optional<String> element() {
    return Optional.ofNullable(element);
  }

  /**
   * Returns the engine's suggestion (for example the field name it guesses was meant).
   *
   * @return the suggestion, or empty
   */
  public Optional<String> suggestion() {
    return Optional.ofNullable(suggestion);
  }
}
