package io.github.underloam.xbbg;

import java.util.Objects;

/** An immutable key/value pair of strings (request elements, kwargs, overrides). */
public final class StringPair {
  private final String key;
  private final String value;

  /**
   * Creates a pair.
   *
   * @param key the key
   * @param value the value
   */
  public StringPair(String key, String value) {
    this.key = Objects.requireNonNull(key, "key");
    this.value = Objects.requireNonNull(value, "value");
  }

  /**
   * Creates a pair.
   *
   * @param key the key
   * @param value the value
   * @return the pair
   */
  public static StringPair of(String key, String value) {
    return new StringPair(key, value);
  }

  /**
   * Returns the key.
   *
   * @return the key
   */
  public String key() {
    return key;
  }

  /**
   * Returns the value.
   *
   * @return the value
   */
  public String value() {
    return value;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof StringPair)) {
      return false;
    }
    StringPair that = (StringPair) other;
    return key.equals(that.key) && value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return 31 * key.hashCode() + value.hashCode();
  }

  @Override
  public String toString() {
    return key + "=" + value;
  }
}
