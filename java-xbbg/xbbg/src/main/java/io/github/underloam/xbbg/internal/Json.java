package io.github.underloam.xbbg.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Strict RFC 8259 JSON codec used for every native argument and JSON result.
 *
 * <p>{@link #parse(String)} returns unmodifiable {@link Map} (insertion-ordered), {@link List},
 * {@link String}, {@link Long} (integers that fit), {@link BigInteger} (larger integers), {@link
 * Double} (numbers with a fraction or exponent), {@link Boolean} or {@code null}. Duplicate object
 * keys, trailing content, unescaped control characters and non-finite numbers are rejected.
 *
 * <p>{@link #write(Object)} accepts {@code null}, {@link CharSequence}, {@link Character}, {@link
 * Boolean}, integral and floating {@link Number}s (finite only), {@link Map} (entries with a {@code
 * null} value are omitted), {@link Iterable}, object and primitive arrays (except {@code byte[]}
 * and {@code char[]}), {@link Optional} and its primitive variants (empty is {@code null}), {@link
 * JsonValue} and the builders. Anything else, including enums, is rejected so that wire values are
 * always explicit. Non-ASCII characters are written as {@code \\uXXXX} escapes, so the text is pure
 * ASCII.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class Json {
  private static final int MAX_DEPTH = 512;
  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private Json() {}

  /**
   * Starts a JSON object.
   *
   * @return a new, empty object builder
   */
  public static JsonObjectBuilder object() {
    return new JsonObjectBuilder();
  }

  /**
   * Starts a JSON array.
   *
   * @return a new, empty array builder
   */
  public static JsonArrayBuilder array() {
    return new JsonArrayBuilder();
  }

  /**
   * Parses one JSON value.
   *
   * @param text the JSON text
   * @return the parsed value (see the class documentation for the Java types)
   * @throws IllegalArgumentException if the text is not valid JSON
   * @throws NullPointerException if {@code text} is null
   */
  public static Object parse(String text) {
    if (text == null) {
      throw new NullPointerException("text");
    }
    Parser parser = new Parser(text);
    parser.skipWhitespace();
    Object value = parser.readValue(0);
    parser.skipWhitespace();
    if (parser.pos != text.length()) {
      throw parser.error("unexpected trailing content");
    }
    return value;
  }

  /**
   * Serializes a value to compact JSON text.
   *
   * @param value the value (see the class documentation for the accepted types)
   * @return the JSON text
   * @throws IllegalArgumentException if the value or a nested value cannot be written
   */
  public static String write(Object value) {
    StringBuilder out = new StringBuilder();
    writeValue(out, value, 0);
    return out.toString();
  }

  /**
   * Quotes a string as a JSON string literal.
   *
   * @param value the string
   * @return the quoted, escaped literal
   */
  public static String quote(String value) {
    StringBuilder out = new StringBuilder(value.length() + 2);
    writeString(out, value);
    return out.toString();
  }

  static void writeValue(StringBuilder out, Object value, int depth) {
    if (depth > MAX_DEPTH) {
      throw new IllegalArgumentException("JSON value is nested deeper than " + MAX_DEPTH);
    }
    if (value == null) {
      out.append("null");
    } else if (value instanceof CharSequence) {
      writeString(out, value.toString());
    } else if (value instanceof Character) {
      writeString(out, value.toString());
    } else if (value instanceof Boolean) {
      out.append(((Boolean) value).booleanValue() ? "true" : "false");
    } else if (value instanceof Number) {
      writeNumber(out, (Number) value);
    } else if (value instanceof JsonValue) {
      out.append(((JsonValue) value).text());
    } else if (value instanceof JsonObjectBuilder) {
      out.append(((JsonObjectBuilder) value).build().text());
    } else if (value instanceof JsonArrayBuilder) {
      out.append(((JsonArrayBuilder) value).build().text());
    } else if (value instanceof Map) {
      writeMap(out, (Map<?, ?>) value, depth);
    } else if (value instanceof Iterable) {
      writeIterable(out, ((Iterable<?>) value).iterator(), depth);
    } else if (value instanceof Optional) {
      writeValue(out, ((Optional<?>) value).orElse(null), depth);
    } else if (value instanceof OptionalInt) {
      OptionalInt optional = (OptionalInt) value;
      writeValue(out, optional.isPresent() ? Integer.valueOf(optional.getAsInt()) : null, depth);
    } else if (value instanceof OptionalLong) {
      OptionalLong optional = (OptionalLong) value;
      writeValue(out, optional.isPresent() ? Long.valueOf(optional.getAsLong()) : null, depth);
    } else if (value instanceof OptionalDouble) {
      OptionalDouble optional = (OptionalDouble) value;
      writeValue(out, optional.isPresent() ? Double.valueOf(optional.getAsDouble()) : null, depth);
    } else if (value.getClass().isArray()) {
      writeArray(out, value, depth);
    } else if (value instanceof Enum) {
      throw new IllegalArgumentException(
          "enum " + value.getClass().getName() + " must be written through its wire value");
    } else {
      throw new IllegalArgumentException(
          "unsupported JSON value type: " + value.getClass().getName());
    }
  }

  /** Whether a value is omitted from objects: {@code null} or an empty optional. */
  static boolean isAbsent(Object value) {
    return value == null
        || (value instanceof Optional && !((Optional<?>) value).isPresent())
        || (value instanceof OptionalInt && !((OptionalInt) value).isPresent())
        || (value instanceof OptionalLong && !((OptionalLong) value).isPresent())
        || (value instanceof OptionalDouble && !((OptionalDouble) value).isPresent());
  }

  private static void writeMap(StringBuilder out, Map<?, ?> map, int depth) {
    out.append('{');
    boolean first = true;
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      Object entryValue = entry.getValue();
      if (isAbsent(entryValue)) {
        continue;
      }
      if (entry.getKey() == null) {
        throw new IllegalArgumentException("JSON object keys must not be null");
      }
      if (!first) {
        out.append(',');
      }
      first = false;
      writeString(out, String.valueOf(entry.getKey()));
      out.append(':');
      writeValue(out, entryValue, depth + 1);
    }
    out.append('}');
  }

  private static void writeIterable(StringBuilder out, Iterator<?> items, int depth) {
    out.append('[');
    boolean first = true;
    while (items.hasNext()) {
      if (!first) {
        out.append(',');
      }
      first = false;
      writeValue(out, items.next(), depth + 1);
    }
    out.append(']');
  }

  private static void writeArray(StringBuilder out, Object array, int depth) {
    if (array instanceof byte[] || array instanceof char[]) {
      throw new IllegalArgumentException(
          array.getClass().getSimpleName() + " has no JSON form; encode it explicitly");
    }
    out.append('[');
    int length = java.lang.reflect.Array.getLength(array);
    for (int i = 0; i < length; i++) {
      if (i > 0) {
        out.append(',');
      }
      writeValue(out, java.lang.reflect.Array.get(array, i), depth + 1);
    }
    out.append(']');
  }

  private static void writeNumber(StringBuilder out, Number number) {
    if (number instanceof Integer
        || number instanceof Long
        || number instanceof Short
        || number instanceof Byte
        || number instanceof BigInteger
        || number instanceof AtomicInteger
        || number instanceof AtomicLong) {
      out.append(number.toString());
    } else if (number instanceof BigDecimal) {
      out.append(((BigDecimal) number).toString());
    } else if (number instanceof Double || number instanceof Float) {
      double value = number.doubleValue();
      if (Double.isNaN(value) || Double.isInfinite(value)) {
        throw new IllegalArgumentException("JSON numbers must be finite, got " + number);
      }
      out.append(formatDouble(value, number instanceof Float));
    } else {
      throw new IllegalArgumentException(
          "unsupported JSON number type: " + number.getClass().getName());
    }
  }

  /**
   * Formats a finite floating-point number the way the engine reads it: integral values below 2^53
   * without a fraction ({@code 2.0} becomes {@code 2}), others in Java's shortest form.
   */
  static String formatDouble(double value, boolean isFloat) {
    // Integral test without ==: compare() treats -0.0 like rint(-0.0); NaN fails the range test.
    if (Math.abs(value) < 9.007199254740992E15 && Double.compare(value, Math.rint(value)) == 0) {
      if (value == 0.0d && 1.0d / value < 0) {
        return "-0.0";
      }
      return Long.toString((long) value);
    }
    return isFloat ? Float.toString((float) value) : Double.toString(value);
  }

  static void writeString(StringBuilder out, String value) {
    out.append('"');
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"':
          out.append("\\\"");
          break;
        case '\\':
          out.append("\\\\");
          break;
        case '\b':
          out.append("\\b");
          break;
        case '\f':
          out.append("\\f");
          break;
        case '\n':
          out.append("\\n");
          break;
        case '\r':
          out.append("\\r");
          break;
        case '\t':
          out.append("\\t");
          break;
        default:
          if (c < 0x20 || c >= 0x7f) {
            out.append("\\u")
                .append(HEX[(c >> 12) & 0xf])
                .append(HEX[(c >> 8) & 0xf])
                .append(HEX[(c >> 4) & 0xf])
                .append(HEX[c & 0xf]);
          } else {
            out.append(c);
          }
      }
    }
    out.append('"');
  }

  private static final class Parser {
    private final String text;
    private int pos;

    Parser(String text) {
      this.text = text;
    }

    IllegalArgumentException error(String message) {
      return new IllegalArgumentException("invalid JSON at offset " + pos + ": " + message);
    }

    void skipWhitespace() {
      while (pos < text.length()) {
        char c = text.charAt(pos);
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
          pos++;
        } else {
          return;
        }
      }
    }

    Object readValue(int depth) {
      if (depth > MAX_DEPTH) {
        throw error("nesting deeper than " + MAX_DEPTH);
      }
      if (pos >= text.length()) {
        throw error("unexpected end of input");
      }
      char c = text.charAt(pos);
      switch (c) {
        case '{':
          return readObject(depth);
        case '[':
          return readArray(depth);
        case '"':
          return readString();
        case 't':
          expectLiteral("true");
          return true;
        case 'f':
          expectLiteral("false");
          return false;
        case 'n':
          expectLiteral("null");
          return null;
        default:
          if (c == '-' || (c >= '0' && c <= '9')) {
            return readNumber();
          }
          throw error("unexpected character '" + printable(c) + "'");
      }
    }

    private void expectLiteral(String literal) {
      if (!text.startsWith(literal, pos)) {
        throw error("expected " + literal);
      }
      pos += literal.length();
    }

    private Map<String, Object> readObject(int depth) {
      pos++;
      LinkedHashMap<String, Object> result = new LinkedHashMap<String, Object>();
      skipWhitespace();
      if (pos < text.length() && text.charAt(pos) == '}') {
        pos++;
        return Collections.unmodifiableMap(result);
      }
      while (true) {
        skipWhitespace();
        if (pos >= text.length() || text.charAt(pos) != '"') {
          throw error("expected a string key");
        }
        int keyStart = pos;
        String key = readString();
        if (result.containsKey(key)) {
          pos = keyStart;
          throw error("duplicate key " + quote(key));
        }
        skipWhitespace();
        if (pos >= text.length() || text.charAt(pos) != ':') {
          throw error("expected ':'");
        }
        pos++;
        skipWhitespace();
        result.put(key, readValue(depth + 1));
        skipWhitespace();
        if (pos >= text.length()) {
          throw error("unexpected end of input in object");
        }
        char c = text.charAt(pos);
        if (c == ',') {
          pos++;
        } else if (c == '}') {
          pos++;
          return Collections.unmodifiableMap(result);
        } else {
          throw error("expected ',' or '}'");
        }
      }
    }

    private List<Object> readArray(int depth) {
      pos++;
      ArrayList<Object> result = new ArrayList<Object>();
      skipWhitespace();
      if (pos < text.length() && text.charAt(pos) == ']') {
        pos++;
        return Collections.unmodifiableList(result);
      }
      while (true) {
        skipWhitespace();
        result.add(readValue(depth + 1));
        skipWhitespace();
        if (pos >= text.length()) {
          throw error("unexpected end of input in array");
        }
        char c = text.charAt(pos);
        if (c == ',') {
          pos++;
        } else if (c == ']') {
          pos++;
          return Collections.unmodifiableList(result);
        } else {
          throw error("expected ',' or ']'");
        }
      }
    }

    private String readString() {
      pos++;
      StringBuilder out = null;
      int runStart = pos;
      while (true) {
        if (pos >= text.length()) {
          throw error("unterminated string");
        }
        char c = text.charAt(pos);
        if (c == '"') {
          String result;
          if (out == null) {
            result = text.substring(runStart, pos);
          } else {
            out.append(text, runStart, pos);
            result = out.toString();
          }
          pos++;
          return result;
        }
        if (c < 0x20) {
          throw error("unescaped control character in string");
        }
        if (c != '\\') {
          pos++;
          continue;
        }
        if (out == null) {
          out = new StringBuilder();
        }
        out.append(text, runStart, pos);
        pos++;
        if (pos >= text.length()) {
          throw error("unterminated escape");
        }
        char escape = text.charAt(pos);
        switch (escape) {
          case '"':
            out.append('"');
            break;
          case '\\':
            out.append('\\');
            break;
          case '/':
            out.append('/');
            break;
          case 'b':
            out.append('\b');
            break;
          case 'f':
            out.append('\f');
            break;
          case 'n':
            out.append('\n');
            break;
          case 'r':
            out.append('\r');
            break;
          case 't':
            out.append('\t');
            break;
          case 'u':
            out.append(readHexChar());
            pos--;
            break;
          default:
            throw error("invalid escape '\\" + printable(escape) + "'");
        }
        pos++;
        runStart = pos;
      }
    }

    private char readHexChar() {
      if (pos + 4 >= text.length()) {
        throw error("truncated \\u escape");
      }
      int value = 0;
      for (int i = 1; i <= 4; i++) {
        char h = text.charAt(pos + i);
        int digit = Character.digit(h, 16);
        if (digit < 0 || h > 'f') {
          throw error("invalid \\u escape");
        }
        value = (value << 4) | digit;
      }
      pos += 5;
      return (char) value;
    }

    private Object readNumber() {
      int start = pos;
      if (text.charAt(pos) == '-') {
        pos++;
      }
      if (pos >= text.length()) {
        throw error("truncated number");
      }
      char first = text.charAt(pos);
      if (first == '0') {
        pos++;
      } else if (first >= '1' && first <= '9') {
        while (pos < text.length() && isDigit(text.charAt(pos))) {
          pos++;
        }
      } else {
        throw error("invalid number");
      }
      boolean integral = true;
      if (pos < text.length() && text.charAt(pos) == '.') {
        integral = false;
        pos++;
        int digits = pos;
        while (pos < text.length() && isDigit(text.charAt(pos))) {
          pos++;
        }
        if (pos == digits) {
          throw error("expected digits after the decimal point");
        }
      }
      if (pos < text.length() && (text.charAt(pos) == 'e' || text.charAt(pos) == 'E')) {
        integral = false;
        pos++;
        if (pos < text.length() && (text.charAt(pos) == '+' || text.charAt(pos) == '-')) {
          pos++;
        }
        int digits = pos;
        while (pos < text.length() && isDigit(text.charAt(pos))) {
          pos++;
        }
        if (pos == digits) {
          throw error("expected exponent digits");
        }
      }
      String literal = text.substring(start, pos);
      if (integral) {
        int digitCount = literal.length() - (literal.charAt(0) == '-' ? 1 : 0);
        if (digitCount < 19) {
          return Long.valueOf(Long.parseLong(literal));
        }
        BigInteger big = new BigInteger(literal);
        if (big.bitLength() < 64) {
          return Long.valueOf(big.longValue());
        }
        return big;
      }
      double value = Double.parseDouble(literal);
      if (Double.isInfinite(value)) {
        pos = start;
        throw error("number out of range: " + literal);
      }
      return Double.valueOf(value);
    }

    private static boolean isDigit(char c) {
      return c >= '0' && c <= '9';
    }

    private static String printable(char c) {
      return c < 0x20 || c >= 0x7f ? String.format("\\u%04x", (int) c) : String.valueOf(c);
    }
  }
}
