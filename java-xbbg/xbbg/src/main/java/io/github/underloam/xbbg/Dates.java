package io.github.underloam.xbbg;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * Date and date-time formatting for request inputs, a port of the JavaScript package's {@code
 * formatDate}/{@code formatDateTime} with {@code java.time} types.
 *
 * <ul>
 *   <li>{@code formatDate} gives Bloomberg-native {@code YYYYMMDD}.
 *   <li>{@code formatDateTime} gives ISO 8601: naive values stay naive (the engine applies the
 *       request time zone), zoned values keep their offset, instants are UTC with milliseconds and
 *       {@code Z}.
 * </ul>
 *
 * <p>Strings: blank text gives {@code null}; {@code YYYYMMDD} passes through; month/day-ambiguous
 * text such as {@code 01/02/2023} is rejected. Naive date-times are never shifted by the host time
 * zone; a zoned value's date is its own calendar date; {@link Instant} and {@link Date} values use
 * their UTC date.
 */
public final class Dates {
  private static final Pattern ISO_DATE = Pattern.compile("^\\d{4}[-/]\\d{2}[-/]\\d{2}$");
  private static final Pattern BBG_DATE = Pattern.compile("^\\d{8}$");
  private static final Pattern AMBIGUOUS =
      Pattern.compile("^\\d{1,2}[-/]\\d{1,2}[-/]\\d{2,4}([T \\D]|$)");
  private static final Pattern SLASH_DATE_TIME = Pattern.compile("^\\d{4}/\\d{2}/\\d{2}T.*");
  private static final DateTimeFormatter BASIC_DATE = DateTimeFormatter.ofPattern("uuuuMMdd");

  private Dates() {}

  /**
   * Formats a date given as text.
   *
   * @param value {@code YYYYMMDD}, {@code YYYY-MM-DD}, {@code YYYY/MM/DD}, or an ISO 8601 date-time
   *     (naive, with an offset, or with a zone)
   * @return {@code YYYYMMDD}, or {@code null} when the value is null or blank
   * @throws IllegalArgumentException if the text is ambiguous or not a date
   */
  public static String formatDate(String value) {
    if (value == null) {
      return null;
    }
    String text = jsTrim(value);
    if (text.isEmpty()) {
      return null;
    }
    if (BBG_DATE.matcher(text).matches()) {
      return text;
    }
    rejectAmbiguous(text);
    if (ISO_DATE.matcher(text).matches()) {
      return text.replace("-", "").replace("/", "");
    }
    return formatDate(parseDateTime(text));
  }

  /**
   * Formats a {@code java.time} value as {@code YYYYMMDD}: its own calendar date when it has one
   * ({@link LocalDate}, {@link LocalDateTime}, {@link ZonedDateTime}, {@link OffsetDateTime}), else
   * its UTC date ({@link Instant}).
   *
   * @param value the value
   * @return {@code YYYYMMDD}, or {@code null} when the value is null
   * @throws IllegalArgumentException if the value has neither a date nor an instant
   */
  public static String formatDate(TemporalAccessor value) {
    if (value == null) {
      return null;
    }
    LocalDate date = value instanceof Instant ? null : value.query(TemporalQueries.localDate());
    if (date == null) {
      if (!value.isSupported(ChronoField.INSTANT_SECONDS)) {
        throw cannotConvert(value);
      }
      date = Instant.from(value).atOffset(ZoneOffset.UTC).toLocalDate();
    }
    return BASIC_DATE.format(date);
  }

  /**
   * Formats a {@link Date} as its UTC calendar date ({@code java.sql.Date}: its local date).
   *
   * @param value the value
   * @return {@code YYYYMMDD}, or {@code null} when the value is null
   */
  @SuppressWarnings("JavaUtilDate")
  public static String formatDate(Date value) {
    // Suppressed: accepting java.util.Date is this public overload's purpose; it is converted to
    // java.time at once.
    if (value == null) {
      return null;
    }
    if ("java.sql.Date".equals(value.getClass().getName())) {
      return formatDate(
          LocalDateTime.ofInstant(Instant.ofEpochMilli(value.getTime()), ZoneId.systemDefault())
              .toLocalDate());
    }
    return formatDate(Instant.ofEpochMilli(value.getTime()));
  }

  /**
   * Formats a date-time given as text. {@code YYYYMMDD} becomes {@code YYYY-MM-DDT00:00:00};
   * anything else is returned with its first space replaced by {@code T}, unvalidated, so naive
   * text stays naive and the engine applies the request time zone.
   *
   * @param value the text
   * @return the ISO text, or {@code null} when the value is null or blank
   * @throws IllegalArgumentException if the text is month/day-ambiguous
   */
  public static String formatDateTime(String value) {
    if (value == null) {
      return null;
    }
    String text = jsTrim(value);
    if (text.isEmpty()) {
      return null;
    }
    rejectAmbiguous(text);
    if (BBG_DATE.matcher(text).matches()) {
      return text.substring(0, 4)
          + "-"
          + text.substring(4, 6)
          + "-"
          + text.substring(6, 8)
          + "T00:00:00";
    }
    return replaceFirstSpace(text);
  }

  /**
   * Formats a {@code java.time} value as ISO 8601: {@link LocalDateTime} naive ({@code
   * 2024-01-17T10:30:00}), {@link LocalDate} as naive midnight, {@link ZonedDateTime} and {@link
   * OffsetDateTime} with their offset ({@code 2024-01-17T10:30:00-05:00}), {@link Instant} in UTC
   * with milliseconds ({@code 2024-01-17T15:30:00.000Z}). Seconds are always present; fractions
   * only when non-zero (instants always show milliseconds).
   *
   * @param value the value
   * @return the ISO text, or {@code null} when the value is null
   * @throws IllegalArgumentException if the value has neither a date nor an instant
   */
  public static String formatDateTime(TemporalAccessor value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Instant) {
      return isoUtc((Instant) value);
    }
    LocalDate date = value.query(TemporalQueries.localDate());
    LocalTime time = value.query(TemporalQueries.localTime());
    ZoneOffset offset = value.query(TemporalQueries.offset());
    if (date != null && time != null) {
      String local = isoLocal(date, time);
      return offset == null ? local : local + offsetText(offset);
    }
    if (date != null) {
      return DateTimeFormatter.ISO_LOCAL_DATE.format(date) + "T00:00:00";
    }
    if (value.isSupported(ChronoField.INSTANT_SECONDS)) {
      return isoUtc(Instant.from(value));
    }
    throw cannotConvert(value);
  }

  /**
   * Formats a {@link Date} as a UTC instant with milliseconds, like JavaScript's {@code
   * toISOString()}.
   *
   * @param value the value
   * @return the ISO text, or {@code null} when the value is null
   */
  @SuppressWarnings("JavaUtilDate")
  public static String formatDateTime(Date value) {
    // Suppressed: accepting java.util.Date is this public overload's purpose; it is converted to
    // java.time at once.
    return value == null ? null : isoUtc(Instant.ofEpochMilli(value.getTime()));
  }

  /**
   * Formats an override or option value that may be a date: {@link TemporalAccessor} and {@link
   * Date} values give {@code YYYYMMDD}; other values give {@code null}.
   */
  static String formatDateValue(Object value) {
    if (value instanceof TemporalAccessor) {
      return formatDate((TemporalAccessor) value);
    }
    if (value instanceof Date) {
      return formatDate((Date) value);
    }
    return null;
  }

  private static TemporalAccessor parseDateTime(String text) {
    String normalized = replaceFirstSpace(text);
    if (SLASH_DATE_TIME.matcher(normalized).matches()) {
      normalized = normalized.substring(0, 10).replace('/', '-') + normalized.substring(10);
    }
    try {
      return OffsetDateTime.parse(normalized);
    } catch (DateTimeParseException notOffset) {
      // Try the next form.
    }
    try {
      return ZonedDateTime.parse(normalized);
    } catch (DateTimeParseException notZoned) {
      // Try the next form.
    }
    try {
      return LocalDateTime.parse(normalized);
    } catch (DateTimeParseException notLocal) {
      throw new IllegalArgumentException(
          "Cannot parse "
              + jsQuote(text)
              + " as a date. Expected ISO 8601, Bloomberg-native YYYYMMDD, or a java.time value.");
    }
  }

  private static void rejectAmbiguous(String text) {
    if (AMBIGUOUS.matcher(text).find() && !ISO_DATE.matcher(jsTrim(text)).matches()) {
      throw new IllegalArgumentException(
          "Ambiguous date format "
              + jsQuote(text)
              + ": month/day order cannot be inferred. Use ISO 8601 (YYYY-MM-DD), Bloomberg-native"
              + " (YYYYMMDD), or pass a Date / Luxon DateTime.");
    }
  }

  private static IllegalArgumentException cannotConvert(TemporalAccessor value) {
    return new IllegalArgumentException(
        "Cannot convert " + value.getClass().getName() + " value " + value + " to a date.");
  }

  private static String isoLocal(LocalDate date, LocalTime time) {
    StringBuilder out = new StringBuilder(DateTimeFormatter.ISO_LOCAL_DATE.format(date));
    out.append('T');
    pad2(out, time.getHour()).append(':');
    pad2(out, time.getMinute()).append(':');
    pad2(out, time.getSecond());
    appendFraction(out, time.getNano(), false);
    return out.toString();
  }

  private static String isoUtc(Instant instant) {
    LocalDateTime utc;
    try {
      utc = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    } catch (DateTimeException e) {
      throw new IllegalArgumentException("instant " + instant + " is out of range", e);
    }
    StringBuilder out = new StringBuilder();
    int year = utc.getYear();
    if (year >= 0 && year <= 9999) {
      out.append(String.format("%04d", year));
    } else {
      out.append(year < 0 ? '-' : '+').append(String.format("%06d", Math.abs(year)));
    }
    out.append('-');
    pad2(out, utc.getMonthValue()).append('-');
    pad2(out, utc.getDayOfMonth()).append('T');
    pad2(out, utc.getHour()).append(':');
    pad2(out, utc.getMinute()).append(':');
    pad2(out, utc.getSecond());
    appendFraction(out, utc.getNano(), true);
    return out.append('Z').toString();
  }

  private static void appendFraction(StringBuilder out, int nanos, boolean alwaysMillis) {
    if (nanos == 0 && !alwaysMillis) {
      return;
    }
    String digits = String.format("%09d", nanos);
    if (nanos % 1_000_000 == 0) {
      out.append('.').append(digits, 0, 3);
    } else if (nanos % 1_000 == 0) {
      out.append('.').append(digits, 0, 6);
    } else {
      out.append('.').append(digits);
    }
  }

  private static String offsetText(ZoneOffset offset) {
    return offset.getTotalSeconds() == 0 ? "Z" : offset.getId();
  }

  private static StringBuilder pad2(StringBuilder out, int value) {
    if (value < 10) {
      out.append('0');
    }
    return out.append(value);
  }

  private static String replaceFirstSpace(String text) {
    int space = text.indexOf(' ');
    return space < 0 ? text : text.substring(0, space) + "T" + text.substring(space + 1);
  }

  /** JavaScript's {@code String.prototype.trim}: WhiteSpace and LineTerminator code points. */
  static String jsTrim(String value) {
    int start = 0;
    int end = value.length();
    while (start < end && isJsWhitespace(value.charAt(start))) {
      start++;
    }
    while (end > start && isJsWhitespace(value.charAt(end - 1))) {
      end--;
    }
    return value.substring(start, end);
  }

  private static boolean isJsWhitespace(char c) {
    return c == '\t'
        || c == '\n'
        || c == '\u000b'
        || c == '\f'
        || c == '\r'
        || c == ' '
        || c == '\u00a0'
        || c == '\u1680'
        || (c >= '\u2000' && c <= '\u200a')
        || c == '\u2028'
        || c == '\u2029'
        || c == '\u202f'
        || c == '\u205f'
        || c == '\u3000'
        || c == '\ufeff';
  }

  /** JavaScript's {@code JSON.stringify} for a string (non-ASCII text stays as is). */
  static String jsQuote(String value) {
    StringBuilder out = new StringBuilder(value.length() + 2).append('"');
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
          boolean loneSurrogate =
              (Character.isHighSurrogate(c)
                      && (i + 1 >= value.length()
                          || !Character.isLowSurrogate(value.charAt(i + 1))))
                  || (Character.isLowSurrogate(c)
                      && (i == 0 || !Character.isHighSurrogate(value.charAt(i - 1))));
          if (c < 0x20 || loneSurrogate) {
            out.append(String.format("\\u%04x", (int) c));
          } else {
            out.append(c);
          }
      }
    }
    return out.append('"').toString();
  }
}
