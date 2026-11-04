package io.github.underloam.xbbg.table;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A view of one row of a {@link Table}. Getters take a column name (the first column with that
 * name) or position and behave like the {@link Column} getters.
 */
public final class Row {
  private final Table table;
  private final int index;

  Row(Table table, int index) {
    this.table = table;
    this.index = index;
  }

  /**
   * Returns the table this row belongs to.
   *
   * @return the table
   */
  public Table table() {
    return table;
  }

  /**
   * Returns the row index.
   *
   * @return the index
   */
  public int index() {
    return index;
  }

  /**
   * Returns a value by column position, boxed as {@link Column#get(int)} does.
   *
   * @param column the column index
   * @return the value, or {@code null}
   */
  public Object get(int column) {
    return table.column(column).get(index);
  }

  /**
   * Returns a value by column name, boxed as {@link Column#get(int)} does.
   *
   * @param column the column name
   * @return the value, or {@code null}
   * @throws IllegalArgumentException if the table has no such column
   */
  public Object get(String column) {
    return table.column(column).get(index);
  }

  /**
   * Whether a value is null.
   *
   * @param column the column name
   * @return {@code true} if the value is null
   */
  public boolean isNull(String column) {
    return table.column(column).isNull(index);
  }

  /**
   * Returns a boolean value; see {@link Column#getBoolean(int)}.
   *
   * @param column the column name
   * @return the value
   */
  public boolean getBoolean(String column) {
    return table.column(column).getBoolean(index);
  }

  /**
   * Returns an integer value; see {@link Column#getInt(int)}.
   *
   * @param column the column name
   * @return the value
   */
  public int getInt(String column) {
    return table.column(column).getInt(index);
  }

  /**
   * Returns a 64-bit integer value; see {@link Column#getLong(int)}.
   *
   * @param column the column name
   * @return the value
   */
  public long getLong(String column) {
    return table.column(column).getLong(index);
  }

  /**
   * Returns a floating-point value; see {@link Column#getDouble(int)}.
   *
   * @param column the column name
   * @return the value
   */
  public double getDouble(String column) {
    return table.column(column).getDouble(index);
  }

  /**
   * Returns a text value; see {@link Column#getString(int)}.
   *
   * @param column the column name
   * @return the value, or {@code null}
   */
  public String getString(String column) {
    return table.column(column).getString(index);
  }

  /**
   * Returns a binary value; see {@link Column#getBytes(int)}.
   *
   * @param column the column name
   * @return a copy of the bytes, or {@code null}
   */
  public byte[] getBytes(String column) {
    return table.column(column).getBytes(index);
  }

  /**
   * Returns a date value; see {@link Column#getLocalDate(int)}.
   *
   * @param column the column name
   * @return the date, or {@code null}
   */
  public LocalDate getLocalDate(String column) {
    return table.column(column).getLocalDate(index);
  }

  /**
   * Returns a time-of-day value; see {@link Column#getLocalTime(int)}.
   *
   * @param column the column name
   * @return the time, or {@code null}
   */
  public LocalTime getLocalTime(String column) {
    return table.column(column).getLocalTime(index);
  }

  /**
   * Returns a timestamp; see {@link Column#getInstant(int)}.
   *
   * @param column the column name
   * @return the instant, or {@code null}
   */
  public Instant getInstant(String column) {
    return table.column(column).getInstant(index);
  }

  /**
   * Returns a timestamp in the column's zone; see {@link Column#getZonedDateTime(int)}.
   *
   * @param column the column name
   * @return the zoned date-time, or {@code null}
   */
  public ZonedDateTime getZonedDateTime(String column) {
    return table.column(column).getZonedDateTime(index);
  }

  /**
   * Returns a timestamp as wall-clock time; see {@link Column#getLocalDateTime(int)}.
   *
   * @param column the column name
   * @return the local date-time, or {@code null}
   */
  public LocalDateTime getLocalDateTime(String column) {
    return table.column(column).getLocalDateTime(index);
  }

  /**
   * Returns the row as a map from column name to boxed value, in column order.
   *
   * @return an unmodifiable map (values may be {@code null})
   */
  public Map<String, Object> toMap() {
    List<Column> columns = table.columns();
    Map<String, Object> map = new LinkedHashMap<String, Object>(columns.size() * 2);
    for (Column column : columns) {
      if (!map.containsKey(column.name())) {
        map.put(column.name(), column.get(index));
      }
    }
    return Collections.unmodifiableMap(map);
  }

  @Override
  public String toString() {
    return "Row" + index + toMap();
  }
}
