package io.github.underloam.xbbg.internal;

/**
 * An immutable, already serialized JSON value: the argument type of every native call.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class JsonValue {
  /** The empty object {@code {}}. */
  public static final JsonValue EMPTY_OBJECT = new JsonValue("{}");

  /** The literal {@code null}. */
  public static final JsonValue NULL = new JsonValue("null");

  private final String text;

  private JsonValue(String text) {
    this.text = text;
  }

  /**
   * Serializes a value with {@link Json#write(Object)}.
   *
   * @param value the value
   * @return the serialized value
   * @throws IllegalArgumentException if the value cannot be written
   */
  public static JsonValue of(Object value) {
    return new JsonValue(Json.write(value));
  }

  /**
   * Wraps JSON text after checking that it parses.
   *
   * @param text the JSON text
   * @return the value
   * @throws IllegalArgumentException if the text is not valid JSON
   */
  public static JsonValue parse(String text) {
    Json.parse(text);
    return new JsonValue(text);
  }

  static JsonValue trusted(String text) {
    return new JsonValue(text);
  }

  /**
   * Returns the JSON text.
   *
   * @return compact JSON text
   */
  public String text() {
    return text;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof JsonValue && ((JsonValue) other).text.equals(text);
  }

  @Override
  public int hashCode() {
    return text.hashCode();
  }

  @Override
  public String toString() {
    return text;
  }
}
