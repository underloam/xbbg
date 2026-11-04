package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.internal.TickBatch;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/**
 * One scalar subscription update: a sparse set of fields of one topic. Immutable and thread-safe.
 *
 * <p>A field is <em>present</em> when the update carries it, possibly as an explicit clear ({@code
 * null}); an absent field did not change. Use {@link #has} and {@link #isNull} to tell the three
 * states apart, since {@link #get} returns {@code null} for both a clear and an absent field.
 *
 * <p>Values arrive already typed by the field's layout kind: {@link Boolean}, {@link Integer}
 * ({@code i32}), {@link Long} ({@code i64}, and {@code time64_us}/{@code timestamp_us} as
 * microseconds), {@link Double}, {@link String} or {@link LocalDate} ({@code date32}). A payload
 * that does not match its field's kind reads as {@code null}.
 */
public final class Tick {
  private static final int ABSENT = -1;

  private final TickBatch batch;
  private final TickLayout layout;
  private final int row;

  /**
   * Layout field index to value position, built on first access. A racy single check: the array is
   * published through the final field of {@link PositionIndex}, so another thread sees it either
   * absent (and builds its own equal copy) or complete.
   */
  private PositionIndex positions;

  Tick(TickBatch batch, TickLayout layout, int row) {
    this.batch = batch;
    this.layout = layout;
    this.row = row;
  }

  /**
   * Returns the topic's consumer label (after aliasing).
   *
   * @return the label
   */
  public String topic() {
    return batch.topic(row);
  }

  /**
   * Returns the update time.
   *
   * @return microseconds since the epoch, UTC
   */
  public long timestampUs() {
    return batch.timestampUs(row);
  }

  /**
   * Returns the version of the row layout this update was decoded with. Layouts grow when fields
   * appear and change when a field's type is promoted.
   *
   * @return the layout version
   */
  public int layoutVersion() {
    return layout.version;
  }

  /**
   * Returns a field's kind in this update's layout: {@code "bool"}, {@code "i32"}, {@code "i64"},
   * {@code "f64"}, {@code "str"}, {@code "date32"}, {@code "time64_us"}, {@code "timestamp_us"} or
   * {@code "unknown"}. Kinds tell apart values that share a Java type, such as {@code i64} and the
   * microsecond times.
   *
   * @param field the field name
   * @return the kind, or empty when the layout has no such field
   */
  public Optional<String> fieldKind(String field) {
    return kindAt(layout.indexOf(field));
  }

  /**
   * Returns a field's kind in this update's layout; see {@link #fieldKind(String)}.
   *
   * @param field the field handle
   * @return the kind, or empty when the layout has no such field
   */
  public Optional<String> fieldKind(FieldHandle field) {
    return kindAt(field.indexIn(layout));
  }

  private Optional<String> kindAt(int index) {
    if (index < 0) {
      return Optional.empty();
    }
    switch (layout.kinds[index]) {
      case TickBatch.KIND_BOOL:
        return Optional.of("bool");
      case TickBatch.KIND_I32:
        return Optional.of("i32");
      case TickBatch.KIND_I64:
        return Optional.of("i64");
      case TickBatch.KIND_F64:
        return Optional.of("f64");
      case TickBatch.KIND_STR:
        return Optional.of("str");
      case TickBatch.KIND_DATE32:
        return Optional.of("date32");
      case TickBatch.KIND_TIME64_US:
        return Optional.of("time64_us");
      case TickBatch.KIND_TIMESTAMP_US:
        return Optional.of("timestamp_us");
      default:
        return Optional.of("unknown");
    }
  }

  /**
   * Whether the update carries a field, including an explicit clear.
   *
   * @param field the field name
   * @return {@code true} if present
   */
  public boolean has(String field) {
    return position(layout.indexOf(field)) != ABSENT;
  }

  /**
   * Whether the update carries a field, including an explicit clear.
   *
   * @param field the field handle
   * @return {@code true} if present
   */
  public boolean has(FieldHandle field) {
    return position(field.indexIn(layout)) != ABSENT;
  }

  /**
   * Whether the update carries a field as an explicit clear (a present {@code null}).
   *
   * @param field the field name
   * @return {@code true} if present and null; {@code false} if absent or valued
   */
  public boolean isNull(String field) {
    return isNullAt(layout.indexOf(field));
  }

  /**
   * Whether the update carries a field as an explicit clear (a present {@code null}).
   *
   * @param field the field handle
   * @return {@code true} if present and null; {@code false} if absent or valued
   */
  public boolean isNull(FieldHandle field) {
    return isNullAt(field.indexIn(layout));
  }

  /**
   * Returns a field's value.
   *
   * @param field the field name
   * @return the value, or {@code null} when cleared or absent (see {@link #has})
   */
  public Object get(String field) {
    return valueOf(layout.indexOf(field));
  }

  /**
   * Returns a field's value.
   *
   * @param field the field handle
   * @return the value, or {@code null} when cleared or absent (see {@link #has})
   */
  public Object get(FieldHandle field) {
    return valueOf(field.indexIn(layout));
  }

  /**
   * Returns a field as a finite double, converting like JavaScript {@code Number(value)}: numbers
   * as-is, booleans as 1 or 0, numeric strings parsed, dates as epoch milliseconds at UTC midnight.
   *
   * @param field the field name
   * @return the number, or empty when absent, cleared, non-numeric or not finite
   */
  public OptionalDouble getDouble(String field) {
    return toDouble(get(field));
  }

  /**
   * Returns a field as a finite double (see {@link #getDouble(String)}).
   *
   * @param field the field handle
   * @return the number, or empty when absent, cleared, non-numeric or not finite
   */
  public OptionalDouble getDouble(FieldHandle field) {
    return toDouble(get(field));
  }

  /**
   * Returns a field as a long, converting like JavaScript {@code BigInt(String(value))}: integers
   * as-is, integral doubles and integer strings parsed.
   *
   * @param field the field name
   * @return the number, or empty when absent, cleared, not an integer or outside the long range
   */
  public OptionalLong getLong(String field) {
    return toLong(get(field));
  }

  /**
   * Returns a field as a long (see {@link #getLong(String)}).
   *
   * @param field the field handle
   * @return the number, or empty when absent, cleared, not an integer or outside the long range
   */
  public OptionalLong getLong(FieldHandle field) {
    return toLong(get(field));
  }

  /**
   * Returns a field as text: strings as-is, numbers formatted like JavaScript {@code
   * String(number)}, booleans as {@code true}/{@code false} and dates in ISO-8601.
   *
   * @param field the field name
   * @return the text, or empty when absent or cleared
   */
  public Optional<String> getString(String field) {
    return toText(get(field));
  }

  /**
   * Returns a field as text (see {@link #getString(String)}).
   *
   * @param field the field handle
   * @return the text, or empty when absent or cleared
   */
  public Optional<String> getString(FieldHandle field) {
    return toText(get(field));
  }

  /**
   * Returns a {@code date32} field.
   *
   * @param field the field name
   * @return the date, or empty when absent, cleared or not a date
   */
  public Optional<LocalDate> getLocalDate(String field) {
    return toDate(get(field));
  }

  /**
   * Returns a {@code date32} field.
   *
   * @param field the field handle
   * @return the date, or empty when absent, cleared or not a date
   */
  public Optional<LocalDate> getLocalDate(FieldHandle field) {
    return toDate(get(field));
  }

  /**
   * Returns {@code timestampUs}, {@code topic}, then every present field in update order (a clear
   * maps to {@code null}). As in JavaScript, a field named {@code timestampUs} or {@code topic}
   * replaces that entry's value.
   *
   * @return an unmodifiable, insertion-ordered map
   */
  public Map<String, Object> toMap() {
    Map<String, Object> out = new LinkedHashMap<String, Object>();
    out.put("timestampUs", timestampUs());
    out.put("topic", topic());
    for (int position = batch.rowStart(row); position < batch.rowEnd(row); position++) {
      int fieldIndex = batch.fieldIndex(position);
      if (fieldIndex < layout.fields.length) {
        out.put(layout.fields[fieldIndex], decode(position, fieldIndex));
      }
    }
    return Collections.unmodifiableMap(out);
  }

  @Override
  public String toString() {
    return "Tick" + toMap();
  }

  private int[] positions() {
    PositionIndex built = positions;
    if (built == null) {
      int[] index = new int[layout.fields.length];
      Arrays.fill(index, ABSENT);
      for (int position = batch.rowStart(row); position < batch.rowEnd(row); position++) {
        int fieldIndex = batch.fieldIndex(position);
        if (fieldIndex < index.length) {
          index[fieldIndex] = position;
        }
      }
      built = new PositionIndex(index);
      positions = built;
    }
    return built.index;
  }

  /** An immutable position index; the final field makes its array safe to share. */
  private static final class PositionIndex {
    final int[] index;

    PositionIndex(int[] index) {
      this.index = index;
    }
  }

  private int position(int fieldIndex) {
    return fieldIndex < 0 ? ABSENT : positions()[fieldIndex];
  }

  private boolean isNullAt(int fieldIndex) {
    int position = position(fieldIndex);
    return position != ABSENT && decode(position, fieldIndex) == null;
  }

  private Object valueOf(int fieldIndex) {
    int position = position(fieldIndex);
    return position == ABSENT ? null : decode(position, fieldIndex);
  }

  private Object decode(int position, int fieldIndex) {
    long bits = batch.valueBits(position);
    switch (batch.valueKind(position)) {
      case TickBatch.VALUE_BOOL:
        return bits != 0;
      case TickBatch.VALUE_I32:
        int number = (int) bits;
        return layout.kinds[fieldIndex] == TickBatch.KIND_DATE32
            ? LocalDate.ofEpochDay(number)
            : Integer.valueOf(number);
      case TickBatch.VALUE_F64:
        return Double.longBitsToDouble(bits);
      case TickBatch.VALUE_STR:
        return batch.string((int) bits);
      case TickBatch.VALUE_I64:
        return bits;
      default:
        return null;
    }
  }

  private static OptionalDouble toDouble(Object value) {
    double number;
    if (value == null) {
      return OptionalDouble.empty();
    } else if (value instanceof Number) {
      number = ((Number) value).doubleValue();
    } else if (value instanceof Boolean) {
      number = ((Boolean) value) ? 1.0 : 0.0;
    } else if (value instanceof LocalDate) {
      number = ((LocalDate) value).toEpochDay() * 86_400_000.0;
    } else {
      number = JsNumbers.parseNumber(value.toString());
    }
    return Double.isNaN(number) || Double.isInfinite(number)
        ? OptionalDouble.empty()
        : OptionalDouble.of(number);
  }

  private static OptionalLong toLong(Object value) {
    Long number;
    if (value == null) {
      return OptionalLong.empty();
    } else if (value instanceof Long || value instanceof Integer) {
      number = ((Number) value).longValue();
    } else if (value instanceof Double) {
      double d = (Double) value;
      // Integral test without ==; NaN and infinities fail the range test first.
      number =
          Math.abs(d) < 1e21 && Double.compare(d, Math.rint(d)) == 0
              ? JsNumbers.toLong(new java.math.BigDecimal(d).toBigInteger())
              : null;
    } else if (value instanceof String) {
      BigInteger parsed = JsNumbers.parseBigInt((String) value);
      number = JsNumbers.toLong(parsed);
    } else {
      number = null;
    }
    return number == null ? OptionalLong.empty() : OptionalLong.of(number);
  }

  private static Optional<String> toText(Object value) {
    if (value == null) {
      return Optional.empty();
    }
    if (value instanceof Double) {
      return Optional.of(JsNumbers.toString((Double) value));
    }
    return Optional.of(value.toString());
  }

  private static Optional<LocalDate> toDate(Object value) {
    return value instanceof LocalDate ? Optional.of((LocalDate) value) : Optional.empty();
  }
}
