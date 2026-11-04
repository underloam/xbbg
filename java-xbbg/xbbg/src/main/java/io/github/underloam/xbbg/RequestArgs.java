package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import io.github.underloam.xbbg.internal.JsonValue;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Argument helpers shared by the typed request options and {@link EngineRequests}. */
final class RequestArgs {
  private RequestArgs() {}

  /**
   * Checks one option value: strings, characters, booleans and finite numbers pass (characters and
   * other char sequences become {@code String}); {@code null} passes only when allowed.
   */
  static Object scalar(String label, Object value, boolean allowNull) {
    if (value == null) {
      if (allowNull) {
        return null;
      }
      throw new IllegalArgumentException(label + " must not be null");
    }
    if (value instanceof CharSequence || value instanceof Character) {
      return value.toString();
    }
    if (value instanceof Boolean) {
      return value;
    }
    if (value instanceof Double || value instanceof Float) {
      double number = ((Number) value).doubleValue();
      if (Double.isNaN(number) || Double.isInfinite(number)) {
        throw new IllegalArgumentException(label + " must be a finite number, got " + value);
      }
      return value;
    }
    if (value instanceof Integer
        || value instanceof Long
        || value instanceof Short
        || value instanceof Byte
        || value instanceof BigInteger
        || value instanceof BigDecimal) {
      return value;
    }
    throw new IllegalArgumentException(
        label + " must be a string, number or boolean, got " + value.getClass().getName());
  }

  /** Copies a map of option values, keeping its iteration order; the copy is unmodifiable. */
  static Map<String, Object> scalars(String name, Map<String, ?> values, boolean allowNull) {
    LinkedHashMap<String, Object> copy = new LinkedHashMap<String, Object>();
    for (Map.Entry<String, ?> entry : values.entrySet()) {
      String key = entry.getKey();
      if (key == null) {
        throw new IllegalArgumentException(name + " keys must not be null");
      }
      copy.put(key, scalar(name + "." + key, entry.getValue(), allowNull));
    }
    return Collections.unmodifiableMap(copy);
  }

  /** Returns {@code values} plus one entry (replacing an existing key in place). */
  static Map<String, Object> withScalar(
      String name, Map<String, Object> values, String key, Object value, boolean allowNull) {
    if (key == null) {
      throw new IllegalArgumentException(name + " keys must not be null");
    }
    LinkedHashMap<String, Object> copy =
        values == null
            ? new LinkedHashMap<String, Object>()
            : new LinkedHashMap<String, Object>(values);
    copy.put(key, scalar(name + "." + key, value, allowNull));
    return Collections.unmodifiableMap(copy);
  }

  private static final String[] DATE_KWARGS = {"startDate", "start_date", "endDate", "end_date"};

  /**
   * Formats the {@code bta} kwargs date entries ({@code startDate}, {@code start_date}, {@code
   * endDate}, {@code end_date}) that hold text, like the TS {@code formatDate} call on them. Blank
   * text stays as it is: it still wins over the typed date options and then sends no date.
   */
  static Map<String, Object> formatDateKwargs(Map<String, Object> kwargs) {
    if (kwargs == null) {
      return null;
    }
    LinkedHashMap<String, Object> copy = null;
    for (String key : DATE_KWARGS) {
      Object value = kwargs.get(key);
      if (!(value instanceof String)) {
        continue;
      }
      String formatted = Dates.formatDate((String) value);
      if (formatted != null && !formatted.equals(value)) {
        if (copy == null) {
          copy = new LinkedHashMap<String, Object>(kwargs);
        }
        copy.put(key, formatted);
      }
    }
    return copy == null ? kwargs : Collections.unmodifiableMap(copy);
  }

  /** Writes study parameters; a {@code null} value is sent as JSON {@code null} (unset). */
  static JsonValue paramsJson(Map<String, Object> params) {
    JsonObjectBuilder object = Json.object();
    for (Map.Entry<String, Object> entry : params.entrySet()) {
      Object value = entry.getValue();
      object.put(entry.getKey(), value == null ? JsonValue.NULL : value);
    }
    return object.build();
  }

  /** Copies a string list; {@code null} items are rejected. */
  static List<String> strings(String name, Collection<String> values) {
    if (values == null) {
      throw new NullPointerException(name);
    }
    ArrayList<String> copy = new ArrayList<String>(values.size());
    for (String value : values) {
      if (value == null) {
        throw new NullPointerException(name + " must not contain null");
      }
      copy.add(value);
    }
    return Collections.unmodifiableList(copy);
  }

  /** Returns a one-item list after checking the item. */
  static List<String> single(String name, String value) {
    if (value == null) {
      throw new NullPointerException(name);
    }
    return Collections.singletonList(value);
  }

  /** Whether two option maps have the same entries in the same order. */
  static boolean orderedEquals(Map<String, Object> left, Map<String, Object> right) {
    if (left == null || right == null) {
      return left == null && right == null;
    }
    return new ArrayList<Map.Entry<String, Object>>(left.entrySet())
        .equals(new ArrayList<Map.Entry<String, Object>>(right.entrySet()));
  }

  /** An order-sensitive hash consistent with {@link #orderedEquals}. */
  static int orderedHash(Map<String, Object> values) {
    return values == null
        ? 0
        : new ArrayList<Map.Entry<String, Object>>(values.entrySet()).hashCode();
  }

  /** {@code Type{name=value, ...}} over the set (non-null) values. */
  static String describe(String type, Object... namesAndValues) {
    StringBuilder out = new StringBuilder(type).append('{');
    boolean first = true;
    for (int i = 0; i + 1 < namesAndValues.length; i += 2) {
      Object value = namesAndValues[i + 1];
      if (value == null) {
        continue;
      }
      if (!first) {
        out.append(", ");
      }
      first = false;
      out.append(namesAndValues[i]).append('=').append(value);
    }
    return out.append('}').toString();
  }

  /** Whether the policy asks the host to emit delayed-data warnings. */
  static boolean warnsOnDelayed(DelayedPolicy policy) {
    return Objects.equals(policy, DelayedPolicy.WARN);
  }
}
