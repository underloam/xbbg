package io.github.underloam.xbbg.table;

import java.util.Objects;
import java.util.Optional;

/** A Bloomberg error attached to a security in the {@code xbbg.security_errors} result metadata. */
public final class BloombergMetadataError {
  private final String category;
  private final String code;
  private final String subcategory;
  private final String message;

  /**
   * Creates a value.
   *
   * @param category Bloomberg's error category, or {@code null}
   * @param code Bloomberg's error code as text, or {@code null}
   * @param subcategory Bloomberg's error subcategory, or {@code null}
   * @param message Bloomberg's message, or {@code null}
   */
  public BloombergMetadataError(String category, String code, String subcategory, String message) {
    this.category = category;
    this.code = code;
    this.subcategory = subcategory;
    this.message = message;
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
    if (!(other instanceof BloombergMetadataError)) {
      return false;
    }
    BloombergMetadataError that = (BloombergMetadataError) other;
    return Objects.equals(category, that.category)
        && Objects.equals(code, that.code)
        && Objects.equals(subcategory, that.subcategory)
        && Objects.equals(message, that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(category, code, subcategory, message);
  }

  @Override
  public String toString() {
    return "BloombergMetadataError{category="
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
