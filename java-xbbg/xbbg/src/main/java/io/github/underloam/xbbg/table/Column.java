package io.github.underloam.xbbg.table;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * One immutable column of a {@link Table}, decoded lazily from canonical little-endian Arrow
 * buffers.
 *
 * <p>Object getters ({@link #get(int)}, {@link #getString(int)}, the {@code java.time} getters)
 * return {@code null} for null values. Primitive getters throw {@link IllegalStateException} for
 * null values; check {@link #isNull(int)} first. A getter that does not fit the column's type
 * throws {@link UnsupportedOperationException}.
 *
 * <p>{@link #get(int)} returns: {@code Boolean}; {@code Byte}, {@code Short}, {@code Integer},
 * {@code Long} for {@code INT8..INT64}; {@code Short}, {@code Integer}, {@code Long}, {@code
 * BigInteger} for {@code UINT8..UINT64} (exact unsigned values); {@code Float}, {@code Double};
 * {@code String} for text; {@code byte[]} (a copy) for binary; {@code LocalDate} for dates; {@code
 * LocalTime} for times; {@code Instant} for timestamps with a time zone and {@code LocalDateTime}
 * for timestamps without one.
 */
public final class Column {
  private static final long NANOS_PER_DAY = 86_400_000_000_000L;
  private static final long MILLIS_PER_DAY = 86_400_000L;

  private final String name;
  private final DataType type;
  private final String timezone;
  private final boolean nullable;
  private final int size;
  private final int nullCount;
  private final byte[] data;
  private final ByteBuffer dataView;
  private final ByteBuffer offsetView;
  private final byte[] validity;
  private volatile ZoneId zone;

  Column(
      String name,
      DataType type,
      String timezone,
      boolean nullable,
      int size,
      int nullCount,
      byte[] data,
      byte[] offsets,
      byte[] validity) {
    this.name = name;
    this.type = type;
    this.timezone = timezone;
    this.nullable = nullable;
    this.size = size;
    this.nullCount = nullCount;
    this.data = data == null ? new byte[0] : data;
    this.dataView = ByteBuffer.wrap(this.data).order(ByteOrder.LITTLE_ENDIAN);
    this.offsetView =
        offsets == null ? null : ByteBuffer.wrap(offsets).order(ByteOrder.LITTLE_ENDIAN);
    this.validity = validity;
  }

  /**
   * Returns the column name.
   *
   * @return the name
   */
  public String name() {
    return name;
  }

  /**
   * Returns the physical type.
   *
   * @return the type
   */
  public DataType type() {
    return type;
  }

  /**
   * Returns the time zone of a timestamp column (an IANA name such as {@code America/New_York},
   * {@code UTC}, or a fixed offset).
   *
   * @return the zone, or empty for other types and for timestamps without a zone
   */
  public Optional<String> timezone() {
    return Optional.ofNullable(timezone);
  }

  /**
   * Whether the schema declares the column nullable.
   *
   * @return the schema flag
   */
  public boolean nullable() {
    return nullable;
  }

  /**
   * Returns the number of values.
   *
   * @return the row count of the table
   */
  public int size() {
    return size;
  }

  /**
   * Returns the number of null values.
   *
   * @return the null count (every value for {@link DataType#NULL})
   */
  public int nullCount() {
    return type == DataType.NULL ? size : nullCount;
  }

  /**
   * Whether a value is null.
   *
   * @param row the row index
   * @return {@code true} if the value is null
   * @throws IndexOutOfBoundsException if the row is out of range
   */
  public boolean isNull(int row) {
    checkRow(row);
    if (type == DataType.NULL) {
      return true;
    }
    return validity != null && (validity[row >>> 3] & (1 << (row & 7))) == 0;
  }

  /**
   * Returns a value boxed as documented on the class.
   *
   * @param row the row index
   * @return the value, or {@code null}
   * @throws IndexOutOfBoundsException if the row is out of range
   */
  public Object get(int row) {
    if (isNull(row)) {
      return null;
    }
    switch (type) {
      case BOOL:
        return Boolean.valueOf(bit(row));
      case INT8:
        return Byte.valueOf(data[row]);
      case INT16:
        return Short.valueOf(dataView.getShort(row * 2));
      case INT32:
        return Integer.valueOf(dataView.getInt(row * 4));
      case INT64:
        return Long.valueOf(dataView.getLong(row * 8));
      case UINT8:
        return Short.valueOf((short) (data[row] & 0xff));
      case UINT16:
        return Integer.valueOf(dataView.getShort(row * 2) & 0xffff);
      case UINT32:
        return Long.valueOf(dataView.getInt(row * 4) & 0xffffffffL);
      case UINT64:
        return unsigned(dataView.getLong(row * 8));
      case FLOAT32:
        return Float.valueOf(dataView.getFloat(row * 4));
      case FLOAT64:
        return Double.valueOf(dataView.getDouble(row * 8));
      case UTF8:
      case LARGE_UTF8:
        return text(row);
      case BINARY:
      case LARGE_BINARY:
        return bytes(row);
      case DATE32:
      case DATE64:
        return date(row);
      case TIME32_S:
      case TIME32_MS:
      case TIME64_US:
      case TIME64_NS:
        return time(row);
      case TIMESTAMP_S:
      case TIMESTAMP_MS:
      case TIMESTAMP_US:
      case TIMESTAMP_NS:
        return timezone == null ? localDateTime(row) : instant(row);
      default:
        throw new IllegalStateException("unhandled type " + type);
    }
  }

  /**
   * Returns a boolean value.
   *
   * @param row the row index
   * @return the value
   * @throws IllegalStateException if the value is null
   * @throws UnsupportedOperationException unless the column is {@link DataType#BOOL}
   */
  public boolean getBoolean(int row) {
    requireType("getBoolean", type == DataType.BOOL);
    requireValue(row);
    return bit(row);
  }

  /**
   * Returns an integer value. Works for every type whose values always fit an {@code int}: {@code
   * INT8}, {@code INT16}, {@code INT32}, {@code UINT8}, {@code UINT16}, {@code DATE32} (days),
   * {@code TIME32_S} and {@code TIME32_MS}.
   *
   * @param row the row index
   * @return the value
   * @throws IllegalStateException if the value is null
   * @throws UnsupportedOperationException for other types
   */
  public int getInt(int row) {
    switch (type) {
      case INT8:
      case INT16:
      case INT32:
      case UINT8:
      case UINT16:
      case DATE32:
      case TIME32_S:
      case TIME32_MS:
        requireValue(row);
        return (int) rawLong(row);
      default:
        throw unsupported("getInt");
    }
  }

  /**
   * Returns a 64-bit integer value. Works for every integer type ({@code UINT64} returns the raw
   * bits; read them with {@link Long#toUnsignedString(long)}) and for the raw value of dates (days
   * or milliseconds), times and timestamps (in the column's unit).
   *
   * @param row the row index
   * @return the value
   * @throws IllegalStateException if the value is null
   * @throws UnsupportedOperationException for boolean, floating-point, text and binary columns
   */
  public long getLong(int row) {
    if (!type.isInteger() && !type.isDate() && !type.isTime() && !type.isTimestamp()) {
      throw unsupported("getLong");
    }
    requireValue(row);
    return rawLong(row);
  }

  /**
   * Returns a float value ({@code FLOAT32}, or an 8- or 16-bit integer).
   *
   * @param row the row index
   * @return the value
   * @throws IllegalStateException if the value is null
   * @throws UnsupportedOperationException for other types
   */
  public float getFloat(int row) {
    switch (type) {
      case FLOAT32:
        requireValue(row);
        return dataView.getFloat(row * 4);
      case INT8:
      case INT16:
      case UINT8:
      case UINT16:
        requireValue(row);
        return rawLong(row);
      default:
        throw unsupported("getFloat");
    }
  }

  /**
   * Returns a floating-point value; integer columns are converted.
   *
   * @param row the row index
   * @return the value
   * @throws IllegalStateException if the value is null
   * @throws UnsupportedOperationException unless the column is numeric
   */
  public double getDouble(int row) {
    if (type == DataType.FLOAT64) {
      requireValue(row);
      return dataView.getDouble(row * 8);
    }
    if (type == DataType.FLOAT32) {
      requireValue(row);
      return dataView.getFloat(row * 4);
    }
    if (type == DataType.UINT64) {
      requireValue(row);
      return unsigned(dataView.getLong(row * 8)).doubleValue();
    }
    if (type.isInteger()) {
      requireValue(row);
      return rawLong(row);
    }
    throw unsupported("getDouble");
  }

  /**
   * Returns a text value.
   *
   * @param row the row index
   * @return the value, or {@code null}
   * @throws UnsupportedOperationException unless the column is {@code UTF8} or {@code LARGE_UTF8}
   */
  public String getString(int row) {
    requireType("getString", type.isText());
    return isNull(row) ? null : text(row);
  }

  /**
   * Returns a copy of a binary or text value's bytes.
   *
   * @param row the row index
   * @return the bytes, or {@code null}
   * @throws UnsupportedOperationException unless the column is binary or text
   */
  public byte[] getBytes(int row) {
    requireType("getBytes", type.isBinary() || type.isText());
    return isNull(row) ? null : bytes(row);
  }

  /**
   * Returns a date value.
   *
   * @param row the row index
   * @return the date, or {@code null}
   * @throws UnsupportedOperationException unless the column is {@code DATE32} or {@code DATE64}
   */
  public LocalDate getLocalDate(int row) {
    requireType("getLocalDate", type.isDate());
    return isNull(row) ? null : date(row);
  }

  /**
   * Returns a time-of-day value.
   *
   * @param row the row index
   * @return the time, or {@code null}
   * @throws UnsupportedOperationException unless the column is a time type
   * @throws DateTimeException if the stored value is outside one day
   */
  public LocalTime getLocalTime(int row) {
    requireType("getLocalTime", type.isTime());
    return isNull(row) ? null : time(row);
  }

  /**
   * Returns a timestamp as an instant. A timestamp without a zone is read as UTC.
   *
   * @param row the row index
   * @return the instant, or {@code null}
   * @throws UnsupportedOperationException unless the column is a timestamp type
   */
  public Instant getInstant(int row) {
    requireType("getInstant", type.isTimestamp());
    return isNull(row) ? null : instant(row);
  }

  /**
   * Returns a timestamp in the column's time zone (UTC when the column has none).
   *
   * @param row the row index
   * @return the zoned date-time, or {@code null}
   * @throws UnsupportedOperationException unless the column is a timestamp type
   * @throws DateTimeException if the column's zone is not a valid zone id
   */
  public ZonedDateTime getZonedDateTime(int row) {
    requireType("getZonedDateTime", type.isTimestamp());
    return isNull(row) ? null : ZonedDateTime.ofInstant(instant(row), zone());
  }

  /**
   * Returns a timestamp as wall-clock time: in the column's zone when it has one, as stored
   * otherwise.
   *
   * @param row the row index
   * @return the local date-time, or {@code null}
   * @throws UnsupportedOperationException unless the column is a timestamp type
   * @throws DateTimeException if the column's zone is not a valid zone id
   */
  public LocalDateTime getLocalDateTime(int row) {
    requireType("getLocalDateTime", type.isTimestamp());
    return isNull(row) ? null : localDateTime(row);
  }

  /**
   * Returns every value boxed as {@link #get(int)} does.
   *
   * @return an unmodifiable list of values (with {@code null} entries)
   */
  public List<Object> toList() {
    List<Object> values = new ArrayList<Object>(size);
    for (int row = 0; row < size; row++) {
      values.add(get(row));
    }
    return Collections.unmodifiableList(values);
  }

  @Override
  public String toString() {
    StringBuilder out = new StringBuilder();
    out.append(name).append(": ").append(type.typeName());
    if (timezone != null) {
      out.append('[').append(timezone).append(']');
    }
    out.append(" (").append(size).append(" values, ").append(nullCount()).append(" null)");
    return out.toString();
  }

  private ZoneId zone() {
    ZoneId result = zone;
    if (result == null) {
      result = timezone == null ? ZoneOffset.UTC : ZoneId.of(timezone);
      zone = result;
    }
    return result;
  }

  private boolean bit(int row) {
    return (data[row >>> 3] & (1 << (row & 7))) != 0;
  }

  private long rawLong(int row) {
    switch (type) {
      case INT8:
        return data[row];
      case UINT8:
        return data[row] & 0xff;
      case INT16:
        return dataView.getShort(row * 2);
      case UINT16:
        return dataView.getShort(row * 2) & 0xffff;
      case INT32:
      case DATE32:
      case TIME32_S:
      case TIME32_MS:
        return dataView.getInt(row * 4);
      case UINT32:
        return dataView.getInt(row * 4) & 0xffffffffL;
      default:
        return dataView.getLong(row * 8);
    }
  }

  private static BigInteger unsigned(long bits) {
    BigInteger value = BigInteger.valueOf(bits & Long.MAX_VALUE);
    return bits < 0 ? value.setBit(63) : value;
  }

  private int start(int row) {
    return type.hasLargeOffsets() ? (int) offsetView.getLong(row * 8) : offsetView.getInt(row * 4);
  }

  private int end(int row) {
    return type.hasLargeOffsets()
        ? (int) offsetView.getLong((row + 1) * 8)
        : offsetView.getInt((row + 1) * 4);
  }

  private String text(int row) {
    int start = start(row);
    return new String(data, start, end(row) - start, StandardCharsets.UTF_8);
  }

  private byte[] bytes(int row) {
    return Arrays.copyOfRange(data, start(row), end(row));
  }

  private LocalDate date(int row) {
    if (type == DataType.DATE32) {
      return LocalDate.ofEpochDay(dataView.getInt(row * 4));
    }
    return LocalDate.ofEpochDay(Math.floorDiv(dataView.getLong(row * 8), MILLIS_PER_DAY));
  }

  private LocalTime time(int row) {
    long nanos;
    switch (type) {
      case TIME32_S:
        nanos = dataView.getInt(row * 4) * 1_000_000_000L;
        break;
      case TIME32_MS:
        nanos = dataView.getInt(row * 4) * 1_000_000L;
        break;
      case TIME64_US:
        {
          // Checked before scaling: a huge value would overflow instead of failing the range test.
          long micros = dataView.getLong(row * 8);
          if (micros < 0 || micros >= NANOS_PER_DAY / 1_000L) {
            throw outsideOneDay(row, micros + " us");
          }
          nanos = micros * 1_000L;
          break;
        }
      default:
        nanos = dataView.getLong(row * 8);
    }
    if (nanos < 0 || nanos >= NANOS_PER_DAY) {
      throw outsideOneDay(row, nanos + " ns");
    }
    return LocalTime.ofNanoOfDay(nanos);
  }

  private DateTimeException outsideOneDay(int row, String value) {
    return new DateTimeException(
        "column '" + name + "' row " + row + " holds a time outside one day: " + value);
  }

  private Instant instant(int row) {
    long value = dataView.getLong(row * 8);
    switch (type) {
      case TIMESTAMP_S:
        return Instant.ofEpochSecond(value);
      case TIMESTAMP_MS:
        return Instant.ofEpochMilli(value);
      case TIMESTAMP_US:
        return Instant.ofEpochSecond(
            Math.floorDiv(value, 1_000_000L), Math.floorMod(value, 1_000_000L) * 1_000L);
      default:
        return Instant.ofEpochSecond(
            Math.floorDiv(value, 1_000_000_000L), Math.floorMod(value, 1_000_000_000L));
    }
  }

  private LocalDateTime localDateTime(int row) {
    return LocalDateTime.ofInstant(instant(row), zone());
  }

  private void requireValue(int row) {
    if (isNull(row)) {
      throw new IllegalStateException("column '" + name + "' is null at row " + row);
    }
  }

  private void requireType(String getter, boolean supported) {
    if (!supported) {
      throw unsupported(getter);
    }
  }

  private UnsupportedOperationException unsupported(String getter) {
    return new UnsupportedOperationException(
        getter + " does not apply to column '" + name + "' of type " + type.typeName());
  }

  private void checkRow(int row) {
    if (row < 0 || row >= size) {
      throw new IndexOutOfBoundsException(
          "row " + row + " is out of range for column '" + name + "' with " + size + " values");
    }
  }
}
