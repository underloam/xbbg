package io.github.underloam.xbbg.internal;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Builds a JSON object in insertion order. Values are serialized when they are put, so later
 * changes to a mutable argument do not leak into the object. {@code null} values are omitted
 * (absent means "not given" on the native side); a key may be put only once.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class JsonObjectBuilder {
  private final StringBuilder out = new StringBuilder("{");
  private final Set<String> keys = new HashSet<String>();

  JsonObjectBuilder() {}

  /**
   * Adds a field unless the value is {@code null}.
   *
   * @param key the field name
   * @param value the value, written with {@link Json#write(Object)}
   * @return this builder
   * @throws IllegalArgumentException if the key was already put or the value cannot be written
   */
  public JsonObjectBuilder put(String key, Object value) {
    if (Json.isAbsent(value)) {
      return this;
    }
    startField(key);
    Json.writeValue(out, value, 1);
    return this;
  }

  /**
   * Adds a boolean field.
   *
   * @param key the field name
   * @param value the value
   * @return this builder
   */
  public JsonObjectBuilder put(String key, boolean value) {
    startField(key);
    out.append(value ? "true" : "false");
    return this;
  }

  /**
   * Adds an integer field.
   *
   * @param key the field name
   * @param value the value
   * @return this builder
   */
  public JsonObjectBuilder put(String key, long value) {
    startField(key);
    out.append(value);
    return this;
  }

  /**
   * Adds a floating-point field.
   *
   * @param key the field name
   * @param value the value; must be finite
   * @return this builder
   * @throws IllegalArgumentException if the value is NaN or infinite
   */
  public JsonObjectBuilder put(String key, double value) {
    if (Double.isNaN(value) || Double.isInfinite(value)) {
      throw new IllegalArgumentException(key + " must be a finite number, got " + value);
    }
    startField(key);
    out.append(Json.formatDouble(value, false));
    return this;
  }

  /**
   * Adds a collection field unless it is {@code null} or empty.
   *
   * @param key the field name
   * @param values the values
   * @return this builder
   */
  public JsonObjectBuilder putNonEmpty(String key, Collection<?> values) {
    if (values == null || values.isEmpty()) {
      return this;
    }
    return put(key, values);
  }

  /**
   * Adds a map field unless it is {@code null} or empty.
   *
   * @param key the field name
   * @param values the entries
   * @return this builder
   */
  public JsonObjectBuilder putNonEmpty(String key, Map<?, ?> values) {
    if (values == null || values.isEmpty()) {
      return this;
    }
    return put(key, values);
  }

  /**
   * Whether a field was already put.
   *
   * @param key the field name
   * @return {@code true} if the object has the field
   */
  public boolean has(String key) {
    return keys.contains(key);
  }

  /**
   * Whether no field was put.
   *
   * @return {@code true} if the object is empty
   */
  public boolean isEmpty() {
    return keys.isEmpty();
  }

  /**
   * Returns the object built so far. The builder stays usable.
   *
   * @return the serialized object
   */
  public JsonValue build() {
    return JsonValue.trusted(out.toString() + "}");
  }

  @Override
  public String toString() {
    return build().text();
  }

  private void startField(String key) {
    if (key == null) {
      throw new IllegalArgumentException("JSON object keys must not be null");
    }
    if (!keys.add(key)) {
      throw new IllegalArgumentException("duplicate JSON key " + Json.quote(key));
    }
    if (keys.size() > 1) {
      out.append(',');
    }
    Json.writeString(out, key);
    out.append(':');
  }
}
