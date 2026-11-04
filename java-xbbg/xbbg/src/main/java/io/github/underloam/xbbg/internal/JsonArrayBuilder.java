package io.github.underloam.xbbg.internal;

/**
 * Builds a JSON array. Values are serialized when they are added; {@code null} is written as the
 * literal {@code null}.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class JsonArrayBuilder {
  private final StringBuilder out = new StringBuilder("[");
  private int size;

  JsonArrayBuilder() {}

  /**
   * Appends a value.
   *
   * @param value the value, written with {@link Json#write(Object)}
   * @return this builder
   * @throws IllegalArgumentException if the value cannot be written
   */
  public JsonArrayBuilder add(Object value) {
    separator();
    Json.writeValue(out, value, 1);
    return this;
  }

  /**
   * Appends a boolean.
   *
   * @param value the value
   * @return this builder
   */
  public JsonArrayBuilder add(boolean value) {
    separator();
    out.append(value ? "true" : "false");
    return this;
  }

  /**
   * Appends an integer.
   *
   * @param value the value
   * @return this builder
   */
  public JsonArrayBuilder add(long value) {
    separator();
    out.append(value);
    return this;
  }

  /**
   * Returns the number of values added so far.
   *
   * @return the array length
   */
  public int size() {
    return size;
  }

  /**
   * Returns the array built so far. The builder stays usable.
   *
   * @return the serialized array
   */
  public JsonValue build() {
    return JsonValue.trusted(out.toString() + "]");
  }

  @Override
  public String toString() {
    return build().text();
  }

  private void separator() {
    if (size > 0) {
      out.append(',');
    }
    size++;
  }
}
