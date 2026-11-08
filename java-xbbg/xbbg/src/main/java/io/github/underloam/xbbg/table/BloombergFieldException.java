package io.github.underloam.xbbg.table;

import java.util.Objects;
import java.util.Optional;

/** A Bloomberg field exception from the {@code xbbg.field_exceptions} result metadata. */
public final class BloombergFieldException {
  private final String field;
  private final String category;
  private final String code;
  private final String subcategory;
  private final String message;

  /**
   * Creates a value.
   *
   * @param field the field id, or {@code null}
   * @param category Bloomberg's error category, or {@code null}
   * @param code Bloomberg's error code as text, or {@code null}
   * @param subcategory Bloomberg's error subcategory, or {@code null}
   * @param message Bloomberg's message, or {@code null}
   */
  public BloombergFieldException(
      String field, String category, String code, String subcategory, String message) {
    this.field = field;
    this.category = category;
    this.code = code;
    this.subcategory = subcategory;
    this.message = message;
  }

  /**
   * Returns the field the exception is about.
   *
   * @return the field id, or empty
   */
  public Optional<String> field() {
    return Optional.ofNullable(field);
  }

  /**
   * Returns the error category.
   *
   * @return the category, or empty
   */
  public Optional<String> category() {
    return Optional.ofNullable(category);
  }

  /**
   * Returns the error code; numeric codes are given in decimal.
   *
   * @return the code, or empty
   */
  public Optional<String> code() {
    return Optional.ofNullable(code);
  }

  /**
   * Returns the error subcategory.
   *
   * @return the subcategory, or empty
   */
  public Optional<String> subcategory() {
    return Optional.ofNullable(subcategory);
  }

  /**
   * Returns Bloomberg's message.
   *
   * @return the message, or empty
   */
  public Optional<String> message() {
    return Optional.ofNullable(message);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof BloombergFieldException)) {
      return false;
    }
    BloombergFieldException that = (BloombergFieldException) other;
    return Objects.equals(field, that.field)
        && Objects.equals(category, that.category)
        && Objects.equals(code, that.code)
        && Objects.equals(subcategory, that.subcategory)
        && Objects.equals(message, that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, category, code, subcategory, message);
  }

  @Override
  public String toString() {
    return "BloombergFieldException{field="
        + field
        + ", category="
        + category
        + ", code="
        + code
        + ", subcategory="
        + subcategory
        + ", message="
        + message
        + "}";
  }
}
