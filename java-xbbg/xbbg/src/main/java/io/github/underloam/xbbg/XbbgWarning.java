package io.github.underloam.xbbg;

import java.util.Objects;
import java.util.Optional;

/**
 * A diagnostic for the caller that is not an error, delivered through {@link XbbgWarnings}.
 *
 * <p>Types and codes: {@code BlpDelayedDataWarning} / {@code XBBG_DELAYED_DATA} (requests) and
 * {@code XBBG_DELAYED_STREAM} (subscriptions), {@code BlpFieldWarning} / {@code
 * XBBG_FIELD_EXCEPTION} (subscriptions), {@code DeprecationWarning} / {@code
 * XBBG_TICKER_KEY_COLUMN}.
 */
public final class XbbgWarning {
  private final String type;
  private final String code;
  private final String message;
  private final String detail;

  /**
   * Creates a warning.
   *
   * @param type the warning type, for example {@code BlpDelayedDataWarning}
   * @param code the warning code, for example {@code XBBG_DELAYED_DATA}
   * @param message the message
   * @param detail additional detail, or {@code null}
   */
  public XbbgWarning(String type, String code, String message, String detail) {
    this.type = Objects.requireNonNull(type, "type");
    this.code = Objects.requireNonNull(code, "code");
    this.message = Objects.requireNonNull(message, "message");
    this.detail = detail;
  }

  /**
   * Returns the warning type.
   *
   * @return the type
   */
  public String type() {
    return type;
  }

  /**
   * Returns the warning code.
   *
   * @return the code
   */
  public String code() {
    return code;
  }

  /**
   * Returns the message.
   *
   * @return the message
   */
  public String message() {
    return message;
  }

  /**
   * Returns additional detail.
   *
   * @return the detail, or empty
   */
  public Optional<String> detail() {
    return Optional.ofNullable(detail);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof XbbgWarning)) {
      return false;
    }
    XbbgWarning that = (XbbgWarning) other;
    return type.equals(that.type)
        && code.equals(that.code)
        && message.equals(that.message)
        && Objects.equals(detail, that.detail);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, code, message, detail);
  }

  @Override
  public String toString() {
    return type + " [" + code + "]: " + message + (detail == null ? "" : " (" + detail + ")");
  }
}
