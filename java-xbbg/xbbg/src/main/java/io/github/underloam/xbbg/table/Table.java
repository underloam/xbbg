package io.github.underloam.xbbg.table;

import io.github.underloam.xbbg.BlpInternalException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * An immutable columnar result: the Java form of an Arrow record batch. Columns decode their values
 * lazily; {@link ResultMetadataView} gives the result metadata.
 */
public final class Table implements Iterable<Row>, ResultMetadataView {
  private final int numRows;
  private final List<Column> columns;
  private final List<String> columnNames;
  private final LazyMetadata metadata;

  private Table(
      int numRows, List<Column> columns, Map<String, String> metadata, String decodedMetadataJson) {
    this.numRows = numRows;
    this.columns = Collections.unmodifiableList(columns);
    List<String> names = new ArrayList<String>(columns.size());
    for (Column column : columns) {
      names.add(column.name());
    }
    this.columnNames = Collections.unmodifiableList(names);
    this.metadata = new LazyMetadata(metadata, decodedMetadataJson);
  }

  /**
   * Builds a table from canonical Arrow buffers: offset 0, little-endian, trailing bits zeroed and
   * offsets starting at the first value. Every array has one entry per column. The arrays are used
   * without copying, so callers must not modify them afterwards.
   *
   * @param names column names
   * @param types {@link DataType} codes
   * @param timezones timestamp zones (or {@code null} entries)
   * @param nullable schema nullability flags
   * @param numRows the row count
   * @param nullCounts null counts
   * @param values value buffers: fixed-width values, bit-packed booleans, or the data bytes of text
   *     and binary columns ({@code null} for {@link DataType#NULL})
   * @param offsets 32-bit (or 64-bit for the large types) offsets of text and binary columns,
   *     {@code null} for other types
   * @param validity bit-packed validity, or {@code null} when the column has no nulls
   * @param metadataKeys schema metadata keys
   * @param metadataValues schema metadata values
   * @param decodedMetadataJson the engine's decoded metadata JSON, or {@code null}
   * @return the table
   * @throws BlpInternalException if the buffers are inconsistent
   */
  public static Table fromArrowBuffers(
      String[] names,
      byte[] types,
      String[] timezones,
      boolean[] nullable,
      int numRows,
      int[] nullCounts,
      byte[][] values,
      byte[][] offsets,
      byte[][] validity,
      String[] metadataKeys,
      String[] metadataValues,
      String decodedMetadataJson) {
    if (names == null || types == null || nullable == null || nullCounts == null) {
      throw new BlpInternalException("native Arrow batch is missing column descriptors");
    }
    int count = names.length;
    if (types.length != count
        || nullable.length != count
        || nullCounts.length != count
        || (timezones != null && timezones.length != count)
        || (values != null && values.length != count)
        || (offsets != null && offsets.length != count)
        || (validity != null && validity.length != count)) {
      throw new BlpInternalException("native Arrow batch has column arrays of different lengths");
    }
    if (numRows < 0) {
      throw new BlpInternalException("native Arrow batch has a negative row count " + numRows);
    }
    List<Column> columns = new ArrayList<Column>(count);
    List<String> unsupported = new ArrayList<String>();
    for (int i = 0; i < count; i++) {
      int code = types[i] & 0xff;
      if (code > DataType.TIMESTAMP_NS.code()) {
        unsupported.add("#" + i + " '" + names[i] + "' (unsupported type code " + code + ")");
      }
    }
    if (!unsupported.isEmpty()) {
      throw new BlpInternalException(
          "native Arrow batch has unsupported columns: " + String.join(", ", unsupported));
    }
    for (int i = 0; i < count; i++) {
      columns.add(
          column(
              names[i],
              DataType.fromCode(types[i] & 0xff),
              timezones == null ? null : timezones[i],
              nullable[i],
              numRows,
              nullCounts[i],
              values == null ? null : values[i],
              offsets == null ? null : offsets[i],
              validity == null ? null : validity[i]));
    }
    return new Table(
        numRows, columns, metadataMap(metadataKeys, metadataValues), decodedMetadataJson);
  }

  private static Column column(
      String name,
      DataType type,
      String timezone,
      boolean nullable,
      int numRows,
      int nullCount,
      byte[] data,
      byte[] offsets,
      byte[] validity) {
    if (name == null) {
      throw new BlpInternalException("native Arrow column has no name");
    }
    if (timezone != null && !type.isTimestamp()) {
      throw new BlpInternalException(
          "native Arrow column " + name + " has a time zone but type " + type.typeName());
    }
    if (nullCount < 0 || nullCount > numRows) {
      throw new BlpInternalException(
          "native Arrow column " + name + " has an invalid null count " + nullCount);
    }
    if (type == DataType.NULL) {
      return new Column(name, type, null, nullable, numRows, numRows, null, null, null);
    }
    long bitmapBytes = (numRows + 7L) / 8L;
    if (type == DataType.BOOL) {
      requireLength(name, "data", data, bitmapBytes);
    } else if (type.isVariableWidth()) {
      long width = type.hasLargeOffsets() ? 8L : 4L;
      requireLength(name, "offsets", offsets, (numRows + 1L) * width);
      checkOffsets(name, type, offsets, numRows, data == null ? 0 : data.length);
    } else {
      requireLength(name, "data", data, numRows * (long) type.byteWidth());
    }
    if (!type.isVariableWidth() && offsets != null) {
      throw new BlpInternalException(
          "native Arrow column " + name + " of type " + type.typeName() + " has offsets");
    }
    if (nullCount > 0) {
      requireLength(name, "validity", validity, bitmapBytes);
      int counted = numRows - countSetBits(validity, numRows);
      if (counted != nullCount) {
        throw new BlpInternalException(
            "native Arrow column "
                + name
                + " declares "
                + nullCount
                + " nulls but its validity bitmap has "
                + counted);
      }
    } else {
      validity = null;
    }
    return new Column(name, type, timezone, nullable, numRows, nullCount, data, offsets, validity);
  }

  private static void requireLength(String name, String buffer, byte[] bytes, long expected) {
    if (expected == 0) {
      return;
    }
    if (bytes == null) {
      throw new BlpInternalException(
          "native Arrow column " + name + " is missing " + buffer + " buffer");
    }
    if (bytes.length < expected) {
      throw new BlpInternalException(
          "native Arrow column "
              + name
              + " "
              + buffer
              + " buffer is too small: expected at least "
              + expected
              + " bytes, got "
              + bytes.length);
    }
  }

  private static void checkOffsets(
      String name, DataType type, byte[] offsets, int numRows, int dataLength) {
    ByteBuffer view = ByteBuffer.wrap(offsets).order(ByteOrder.LITTLE_ENDIAN);
    boolean large = type.hasLargeOffsets();
    long previous = 0;
    for (int i = 0; i <= numRows; i++) {
      long offset = large ? view.getLong(i * 8) : view.getInt(i * 4);
      if (offset < previous || (i == 0 && offset != 0) || offset > dataLength) {
        throw new BlpInternalException(
            "native Arrow column " + name + " has invalid offset " + offset + " at index " + i);
      }
      previous = offset;
    }
  }

  private static int countSetBits(byte[] bitmap, int bits) {
    int full = bits >>> 3;
    int set = 0;
    for (int i = 0; i < full; i++) {
      set += Integer.bitCount(bitmap[i] & 0xff);
    }
    int rest = bits & 7;
    if (rest != 0) {
      set += Integer.bitCount(bitmap[full] & ((1 << rest) - 1));
    }
    return set;
  }

  private static Map<String, String> metadataMap(String[] keys, String[] values) {
    if (keys == null && values == null) {
      return Collections.emptyMap();
    }
    if (keys == null || values == null || keys.length != values.length) {
      throw new BlpInternalException("native Arrow batch has mismatched metadata arrays");
    }
    Map<String, String> map = new LinkedHashMap<String, String>();
    for (int i = 0; i < keys.length; i++) {
      if (keys[i] == null || values[i] == null) {
        throw new BlpInternalException("native Arrow batch has a null metadata entry");
      }
      map.put(keys[i], values[i]);
    }
    return Collections.unmodifiableMap(map);
  }

  /**
   * Returns the number of rows.
   *
   * @return the row count
   */
  public int numRows() {
    return numRows;
  }

  /**
   * Returns the number of columns.
   *
   * @return the column count
   */
  public int numColumns() {
    return columns.size();
  }

  /**
   * Returns the columns in schema order.
   *
   * @return an unmodifiable list
   */
  public List<Column> columns() {
    return columns;
  }

  /**
   * Returns the column names in schema order.
   *
   * @return an unmodifiable list
   */
  public List<String> columnNames() {
    return columnNames;
  }

  /**
   * Returns a column by position.
   *
   * @param index the column index
   * @return the column
   * @throws IndexOutOfBoundsException if the index is out of range
   */
  public Column column(int index) {
    return columns.get(index);
  }

  /**
   * Returns the first column with a name.
   *
   * @param name the column name
   * @return the column
   * @throws IllegalArgumentException if the table has no such column
   */
  public Column column(String name) {
    Optional<Column> column = findColumn(name);
    if (!column.isPresent()) {
      throw new IllegalArgumentException("no column '" + name + "'; columns are " + columnNames);
    }
    return column.get();
  }

  /**
   * Looks up the first column with a name.
   *
   * @param name the column name
   * @return the column, or empty
   */
  public Optional<Column> findColumn(String name) {
    for (Column column : columns) {
      if (column.name().equals(name)) {
        return Optional.of(column);
      }
    }
    return Optional.empty();
  }

  /**
   * Whether the table has a column with a name.
   *
   * @param name the column name
   * @return {@code true} if such a column exists
   */
  public boolean hasColumn(String name) {
    return columnNames.contains(name);
  }

  /**
   * Returns a row view.
   *
   * @param index the row index
   * @return the row
   * @throws IndexOutOfBoundsException if the index is out of range
   */
  public Row row(int index) {
    if (index < 0 || index >= numRows) {
      throw new IndexOutOfBoundsException(
          "row " + index + " is out of range for a table with " + numRows + " rows");
    }
    return new Row(this, index);
  }

  /**
   * Iterates over the rows.
   *
   * @return a row iterator
   */
  @Override
  public Iterator<Row> iterator() {
    return new Iterator<Row>() {
      private int next;

      @Override
      public boolean hasNext() {
        return next < numRows;
      }

      @Override
      public Row next() {
        if (next >= numRows) {
          throw new NoSuchElementException();
        }
        return new Row(Table.this, next++);
      }
    };
  }

  /**
   * Converts the table to row maps, as the {@code ROWS} backend does.
   *
   * @return the rows with the same metadata
   */
  public RowsResult toRows() {
    return RowsResult.of(this);
  }

  /**
   * Returns the raw schema metadata.
   *
   * @return an unmodifiable map
   */
  @Override
  public Map<String, String> metadata() {
    return metadata.raw();
  }

  LazyMetadata lazyMetadata() {
    return metadata;
  }

  /**
   * Returns the decoded result metadata, decoding it on first use.
   *
   * @return the metadata
   * @throws BlpInternalException if the engine's decoded metadata is malformed
   */
  @Override
  public ResultMetadata resultMetadata() {
    return metadata.get();
  }

  @Override
  public String toString() {
    StringBuilder out = new StringBuilder("Table[");
    out.append(numRows).append(numRows == 1 ? " row" : " rows");
    for (int i = 0; i < columns.size(); i++) {
      Column column = columns.get(i);
      out.append(i == 0 ? ": " : ", ").append(column.name()).append(' ').append(column.type());
    }
    return out.append(']').toString();
  }
}
