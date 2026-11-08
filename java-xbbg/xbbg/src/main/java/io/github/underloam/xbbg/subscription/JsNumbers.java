package io.github.underloam.xbbg.subscription;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.regex.Pattern;

/**
 * The JavaScript conversions behind the {@link Tick} typed getters: {@code Number(text)}, {@code
 * BigInt(text)} and {@code String(number)}.
 */
final class JsNumbers {
  private static final Pattern DECIMAL =
      Pattern.compile("[+-]?(?:\\d+\\.?\\d*(?:[eE][+-]?\\d+)?|\\.\\d+(?:[eE][+-]?\\d+)?)");
  private static final Pattern DECIMAL_INTEGER = Pattern.compile("[+-]?\\d+");
  private static final BigInteger LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
  private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);

  private JsNumbers() {}

  /** ECMAScript white space and line terminators, as {@code String.prototype.trim} removes. */
  static boolean isJsWhitespace(char c) {
    switch (c) {
      case '\t':
      case '\n':
      case '\u000b':
      case '\u000c':
      case '\r':
      case ' ':
      case '\u00a0':
      case '\u1680':
      case '\u2028':
      case '\u2029':
      case '\u202f':
      case '\u205f':
      case '\u3000':
      case '\ufeff':
        return true;
      default:
        return c >= '\u2000' && c <= '\u200a';
    }
  }

  static String trim(String text) {
    int start = 0;
    int end = text.length();
    while (start < end && isJsWhitespace(text.charAt(start))) {
      start++;
    }
    while (end > start && isJsWhitespace(text.charAt(end - 1))) {
      end--;
    }
    return text.substring(start, end);
  }

  /** {@code Number(text)}: NaN when the text is not a numeric literal. */
  static double parseNumber(String text) {
    String value = trim(text);
    if (value.isEmpty()) {
      return 0.0;
    }
    BigInteger prefixed = parsePrefixed(value);
    if (prefixed != null) {
      return prefixed.doubleValue();
    }
    switch (value) {
      case "Infinity":
      case "+Infinity":
        return Double.POSITIVE_INFINITY;
      case "-Infinity":
        return Double.NEGATIVE_INFINITY;
      default:
        break;
    }
    if (!DECIMAL.matcher(value).matches()) {
      return Double.NaN;
    }
    return Double.parseDouble(value);
  }

  /** {@code BigInt(text)}: {@code null} when the text is not an integer literal. */
  static BigInteger parseBigInt(String text) {
    String value = trim(text);
    if (value.isEmpty()) {
      return BigInteger.ZERO;
    }
    BigInteger prefixed = parsePrefixed(value);
    if (prefixed != null) {
      return prefixed;
    }
    if (!DECIMAL_INTEGER.matcher(value).matches()) {
      return null;
    }
    return new BigInteger(value.startsWith("+") ? value.substring(1) : value);
  }

  /** {@code 0x}, {@code 0o} and {@code 0b} literals (unsigned, as JavaScript accepts them). */
  private static BigInteger parsePrefixed(String value) {
    if (value.length() < 3 || value.charAt(0) != '0') {
      return null;
    }
    int radix;
    switch (value.charAt(1)) {
      case 'x':
      case 'X':
        radix = 16;
        break;
      case 'o':
      case 'O':
        radix = 8;
        break;
      case 'b':
      case 'B':
        radix = 2;
        break;
      default:
        return null;
    }
    String digits = value.substring(2);
    for (int i = 0; i < digits.length(); i++) {
      if (Character.digit(digits.charAt(i), radix) < 0) {
        return null;
      }
    }
    return new BigInteger(digits, radix);
  }

  /** The value as a {@code long} when it fits, else {@code null}. */
  static Long toLong(BigInteger value) {
    if (value == null || value.compareTo(LONG_MIN) < 0 || value.compareTo(LONG_MAX) > 0) {
      return null;
    }
    return value.longValue();
  }

  /** {@code String(number)} (ECMAScript {@code Number::toString}). */
  static String toString(double value) {
    if (Double.isNaN(value)) {
      return "NaN";
    }
    if (Double.isInfinite(value)) {
      return value > 0 ? "Infinity" : "-Infinity";
    }
    if (value == 0.0) {
      return "0";
    }
    BigDecimal decimal = new BigDecimal(Double.toString(Math.abs(value))).stripTrailingZeros();
    String digits = decimal.unscaledValue().toString();
    int k = digits.length();
    int n = k - decimal.scale();
    StringBuilder out = new StringBuilder(value < 0 ? "-" : "");
    if (k <= n && n <= 21) {
      out.append(digits);
      for (int i = k; i < n; i++) {
        out.append('0');
      }
    } else if (0 < n && n <= 21) {
      out.append(digits, 0, n).append('.').append(digits, n, k);
    } else if (-6 < n && n <= 0) {
      out.append("0.");
      for (int i = n; i < 0; i++) {
        out.append('0');
      }
      out.append(digits);
    } else {
      out.append(digits.charAt(0));
      if (k > 1) {
        out.append('.').append(digits, 1, k);
      }
      int exponent = n - 1;
      out.append('e').append(exponent < 0 ? '-' : '+').append(Math.abs(exponent));
    }
    return out.toString();
  }
}
