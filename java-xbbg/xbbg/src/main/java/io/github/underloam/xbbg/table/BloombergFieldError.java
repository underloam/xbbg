package io.github.underloam.xbbg.table;

import java.util.Objects;
import java.util.Optional;

/**
 * Bloomberg's refusal of one field id in a field-info request, from the {@code xbbg.field_errors}
 * result metadata.
 */
public final class BloombergFieldError {
  private final String category;
  private final int code;
  private final String subcategory;
  private final String message;
  private final String source;

  /**
   * Creates a value.
   *
   * @param category Bloomberg's error category
   * @param code Bloomberg's error code
   * @param subcategory Bloomberg's error subcategory
   * @param message Bloomberg's message
   * @param source the error source, or {@code null}
   */
  public BloombergFieldError(
      String category, int code, String subcategory, String message, String source) {
    this.category = Objects.requireNonNull(category, "category");
    this.code = code;
    this.subcategory = Objects.requireNonNull(subcategory, "subcategory");
    this.message = Objects.requireNonNull(message, "message");
    this.source = source;
  }

  /**
   * Returns the error category.
   *
   * @return the category
   */
  public String category() {
    return category;
  }

  /**
   * Returns the error code.
   *
   * @return the code
   */
  public int code() {
    return code;
  }

  /**
   * Returns the error subcategory.
   *
   * @return the subcategory
   */
  public String subcategory() {
    return subcategory;
  }

  /**
   * Returns Bloomberg's message.
   *
   * @return the message
   */
  public String message() {
    return message;
  }

  /**
   * Returns the error source.
   *
   * @return the source, or empty
   */
  public Optional<String> source() {
    return Optional.ofNullable(source);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof BloombergFieldError)) {
      return false;
    }
    BloombergFieldError that = (BloombergFieldError) other;
    return code == that.code
        && category.equals(that.category)
        && subcategory.equals(that.subcategory)
        && message.equals(that.message)
        && Objects.equals(source, that.source);
  }

  @Override
  public int hashCode() {
    return Objects.hash(category, code, subcategory, message, source);
  }

  @Override
  public String toString() {
    return "BloombergFieldError{category="
        + category
        + ", code="
        + code
        + ", subcategory="
        + subcategory
        + ", message="
        + message
        + ", source="
        + source
        + "}";
  }
}
