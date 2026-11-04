package io.github.underloam.xbbg.internal;

import io.github.underloam.xbbg.BlpInternalException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Typed access to values produced by {@link Json#parse(String)} from native JSON results. A value
 * of the wrong shape means the native library broke its contract, so every accessor throws {@link
 * BlpInternalException} ({@code malformed native JSON: ...}).
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class JsonFields {
  private JsonFields() {}

  /**
   * Parses native JSON text.
   *
   * @param text the JSON text
   * @param what a description of the value for error messages
   * @return the parsed value
   * @throws BlpInternalException if the text is not valid JSON
   */
  public static Object parse(String text, String what) {
    if (text == null) {
      throw malformed(what, "is missing");
    }
    try {
      return Json.parse(text);
    } catch (IllegalArgumentException e) {
      throw new BlpInternalException(
          "malformed native JSON for " + what + ": " + e.getMessage(), e);
    }
  }

  /**
   * Casts a parsed value to an object.
   *
   * @param value the parsed value
   * @param what a description of the value for error messages
   * @return the object
   */
  // Unchecked: values come from Json.parse, whose objects are unmodifiable Map<String, Object>
  // (keys are always strings); callers only read them.
  @SuppressWarnings("unchecked")
  public static Map<String, Object> object(Object value, String what) {
    if (value instanceof Map) {
      return (Map<String, Object>) value;
    }
    throw malformed(what, "must be an object");
  }

  /**
   * Casts a parsed value to an array.
   *
   * @param value the parsed value
   * @param what a description of the value for error messages
   * @return the list
   */
  // Unchecked: values come from Json.parse, whose arrays are unmodifiable List<Object>; callers
  // only read them.
  @SuppressWarnings("unchecked")
  public static List<Object> array(Object value, String what) {
    if (value instanceof List) {
      return (List<Object>) value;
    }
    throw malformed(what, "must be an array");
  }

  /**
   * Converts a parsed array of strings.
   *
   * @param value the parsed value
   * @param what a description of the value for error messages
   * @return an unmodifiable list of strings
   */
  public static List<String> stringList(Object value, String what) {
    List<Object> items = array(value, what);
    List<String> result = new ArrayList<String>(items.size());
    for (Object item : items) {
      if (!(item instanceof String)) {
        throw malformed(what, "must hold only strings");
      }
      result.add((String) item);
    }
    return Collections.unmodifiableList(result);
  }

  /**
   * Converts a parsed integer.
   *
   * @param value the parsed value
   * @param what a description of the value for error messages
   * @return the value
   */
  public static long longOf(Object value, String what) {
    if (value instanceof Long) {
      return ((Long) value).longValue();
    }
    if (value instanceof BigInteger) {
      throw malformed(what, "is out of the 64-bit range");
    }
    throw malformed(what, "must be an integer");
  }

  /**
   * Converts a parsed number.
   *
   * @param value the parsed value
   * @param what a description of the value for error messages
   * @return the value
   */
  public static double doubleOf(Object value, String what) {
    if (value instanceof Double || value instanceof Long || value instanceof BigInteger) {
      return ((Number) value).doubleValue();
    }
    throw malformed(what, "must be a number");
  }

  /**
   * Reads a required string field.
   *
   * @param object the object
   * @param key the field name
   * @return the value
   */
  public static String string(Map<String, Object> object, String key) {
    Object value = object.get(key);
    if (value instanceof String) {
      return (String) value;
    }
    throw malformed(key, value == null ? "is missing" : "must be a string");
  }

  /**
   * Reads an optional string field ({@code null} when absent or JSON {@code null}).
   *
   * @param object the object
   * @param key the field name
   * @return the value, or {@code null}
   */
  public static String optString(Map<String, Object> object, String key) {
    Object value = object.get(key);
    if (value == null || value instanceof String) {
      return (String) value;
    }
    throw malformed(key, "must be a string");
  }

  /**
   * Reads a required integer field.
   *
   * @param object the object
   * @param key the field name
   * @return the value
   */
  public static long longValue(Map<String, Object> object, String key) {
    Object value = object.get(key);
    if (value == null) {
      throw malformed(key, "is missing");
    }
    return longOf(value, key);
  }

  /**
   * Reads an optional integer field.
   *
   * @param object the object
   * @param key the field name
   * @return the value, or {@code null}
   */
  public static Long optLong(Map<String, Object> object, String key) {
    Object value = object.get(key);
    return value == null ? null : Long.valueOf(longOf(value, key));
  }

  /**
   * Reads a required 32-bit integer field.
   *
   * @param object the object
   * @param key the field name
   * @return the value
   */
  public static int intValue(Map<String, Object> object, String key) {
    long value = longValue(object, key);
    if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
      throw malformed(key, "is out of the 32-bit range");
    }
    return (int) value;
  }

  /**
   * Reads a required number field.
   *
   * @param object the object
   * @param key the field name
   * @return the value
   */
  public static double doubleValue(Map<String, Object> object, String key) {
    Object value = object.get(key);
    if (value == null) {
      throw malformed(key, "is missing");
    }
    return doubleOf(value, key);
  }

  /**
   * Reads an optional number field.
   *
   * @param object the object
   * @param key the field name
   * @return the value, or {@code null}
   */
  public static Double optDouble(Map<String, Object> object, String key) {
    Object value = object.get(key);
    return value == null ? null : Double.valueOf(doubleOf(value, key));
  }

  /**
   * Reads a required boolean field.
   *
   * @param object the object
   * @param key the field name
   * @return the value
   */
  public static boolean bool(Map<String, Object> object, String key) {
    Object value = object.get(key);
    if (value instanceof Boolean) {
      return ((Boolean) value).booleanValue();
    }
    throw malformed(key, value == null ? "is missing" : "must be a boolean");
  }

  /**
   * Reads an optional boolean field.
   *
   * @param object the object
   * @param key the field name
   * @return the value, or {@code null}
   */
  public static Boolean optBool(Map<String, Object> object, String key) {
    Object value = object.get(key);
    if (value == null || value instanceof Boolean) {
      return (Boolean) value;
    }
    throw malformed(key, "must be a boolean");
  }

  /**
   * Reads a required array-of-strings field.
   *
   * @param object the object
   * @param key the field name
   * @return an unmodifiable list
   */
  public static List<String> strings(Map<String, Object> object, String key) {
    Object value = object.get(key);
    if (value == null) {
      throw malformed(key, "is missing");
    }
    return stringList(value, key);
  }

  /**
   * Reads an optional array-of-strings field.
   *
   * @param object the object
   * @param key the field name
   * @return an unmodifiable list, or {@code null}
   */
  public static List<String> optStrings(Map<String, Object> object, String key) {
    Object value = object.get(key);
    return value == null ? null : stringList(value, key);
  }

  /**
   * Reads a required object field.
   *
   * @param object the object
   * @param key the field name
   * @return the nested object
   */
  public static Map<String, Object> obj(Map<String, Object> object, String key) {
    Object value = object.get(key);
    if (value == null) {
      throw malformed(key, "is missing");
    }
    return object(value, key);
  }

  /**
   * Reads an optional object field.
   *
   * @param object the object
   * @param key the field name
   * @return the nested object, or {@code null}
   */
  public static Map<String, Object> optObj(Map<String, Object> object, String key) {
    Object value = object.get(key);
    return value == null ? null : object(value, key);
  }

  /**
   * Reads a required array field.
   *
   * @param object the object
   * @param key the field name
   * @return the list
   */
  public static List<Object> list(Map<String, Object> object, String key) {
    Object value = object.get(key);
    if (value == null) {
      throw malformed(key, "is missing");
    }
    return array(value, key);
  }

  /**
   * Reads an optional array field.
   *
   * @param object the object
   * @param key the field name
   * @return the list, or {@code null}
   */
  public static List<Object> optList(Map<String, Object> object, String key) {
    Object value = object.get(key);
    return value == null ? null : array(value, key);
  }

  /**
   * Builds the exception for a malformed native value.
   *
   * @param what the value description
   * @param problem what is wrong
   * @return the exception to throw
   */
  public static BlpInternalException malformed(String what, String problem) {
    return new BlpInternalException("malformed native JSON: " + what + " " + problem);
  }
}
