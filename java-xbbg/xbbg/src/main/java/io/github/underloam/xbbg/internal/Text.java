package io.github.underloam.xbbg.internal;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Converts argument values to the request strings the engine receives, the way JavaScript's {@code
 * String(value)} does for strings, numbers and booleans, and redacts credentials from URLs shown in
 * messages.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class Text {
  private static final String REDACTED = "***";

  private Text() {}

  /**
   * Replaces the user-info ({@code user:password@}) of every {@code scheme://} URL in a text with
   * {@code ***@}, for messages and {@code toString()} output. Everything else is unchanged.
   *
   * @param text the text, for example a source spec or a proxy URL
   * @return the text without URL credentials
   */
  public static String redactUserInfo(String text) {
    StringBuilder out = null;
    int copied = 0;
    int search = 0;
    int scheme;
    while ((scheme = text.indexOf("://", search)) >= 0) {
      int authority = scheme + 3;
      int end = authority;
      while (end < text.length() && "/?#".indexOf(text.charAt(end)) < 0) {
        end++;
      }
      // The user-info ends at the last '@' of the authority (a password may contain '@').
      int at = text.lastIndexOf('@', end - 1);
      if (at >= authority) {
        if (out == null) {
          out = new StringBuilder(text.length());
        }
        out.append(text, copied, authority).append(REDACTED);
        copied = at;
      }
      search = end;
    }
    return out == null ? text : out.append(text, copied, text.length()).toString();
  }

  /**
   * Converts a value to request text. Numbers follow JavaScript's {@code Number.prototype
   * .toString()}: {@code 40.0} is {@code "40"}, {@code 1e-7} is {@code "1e-7"}, {@code 1e21} is
   * {@code "1e+21"}. Every other non-null value uses {@link String#valueOf(Object)}.
   *
   * @param value the value
   * @return the text
   * @throws NullPointerException if the value is null
   */
  public static String toRequestString(Object value) {
    if (value == null) {
      throw new NullPointerException("value");
    }
    if (value instanceof Double) {
      return jsNumber(((Double) value).doubleValue(), Double.toString(Math.abs((Double) value)));
    }
    if (value instanceof Float) {
      float f = ((Float) value).floatValue();
      return jsNumber(f, Float.toString(Math.abs(f)));
    }
    if (value instanceof BigDecimal) {
      return ((BigDecimal) value).toPlainString();
    }
    return String.valueOf(value);
  }

  private static String jsNumber(double value, String shortestAbs) {
    if (Double.isNaN(value)) {
      return "NaN";
    }
    if (Double.isInfinite(value)) {
      return value > 0 ? "Infinity" : "-Infinity";
    }
    if (value == 0.0d) {
      return "0";
    }
    BigDecimal decimal = new BigDecimal(shortestAbs).stripTrailingZeros();
    BigInteger unscaled = decimal.unscaledValue();
    String digits = unscaled.toString();
    int k = digits.length();
    int n = k - decimal.scale();
    StringBuilder out = new StringBuilder();
    if (value < 0) {
      out.append('-');
    }
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
      int exponent = n - 1;
      out.append(digits.charAt(0));
      if (k > 1) {
        out.append('.').append(digits, 1, k);
      }
      out.append('e').append(exponent < 0 ? '-' : '+').append(Math.abs(exponent));
    }
    return out.toString();
  }
}
