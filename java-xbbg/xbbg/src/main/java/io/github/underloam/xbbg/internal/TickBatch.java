package io.github.underloam.xbbg.internal;

/**
 * One layout-homogeneous batch of scalar subscription rows in the columnar form the native library
 * sends (contract §7).
 *
 * <p>Row {@code r} owns value positions {@code rowOffsets[r] .. rowOffsets[r + 1]}. Each position
 * names a layout field ({@code fieldIndices}) and carries a value already resolved for that field's
 * layout kind: {@code valueKinds} says how to read {@code valueBits} (booleans and integers as
 * {@code long}, doubles as raw bits, strings as an index into {@code strings}). A position with
 * kind {@link #VALUE_NULL} is an explicit clear; a field without a position is absent from the
 * update. The layout is present only when it differs from the previous batch's.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class TickBatch {
  /** Value kind: explicit clear (or a payload that does not match the layout kind). */
  public static final byte VALUE_NULL = 0;

  /** Value kind: boolean, {@code bits != 0}. */
  public static final byte VALUE_BOOL = 1;

  /** Value kind: 32-bit integer (days since the epoch for a {@code date32} field). */
  public static final byte VALUE_I32 = 2;

  /** Value kind: double, {@link Double#longBitsToDouble(long)} of the bits. */
  public static final byte VALUE_F64 = 3;

  /** Value kind: string, {@code strings[(int) bits]}. */
  public static final byte VALUE_STR = 4;

  /** Value kind: 64-bit integer (also {@code time64_us} and {@code timestamp_us} fields). */
  public static final byte VALUE_I64 = 5;

  /** Layout kind {@code unknown}. */
  public static final byte KIND_UNKNOWN = 0;

  /** Layout kind {@code bool}. */
  public static final byte KIND_BOOL = 1;

  /** Layout kind {@code i32}. */
  public static final byte KIND_I32 = 2;

  /** Layout kind {@code i64}. */
  public static final byte KIND_I64 = 3;

  /** Layout kind {@code f64}. */
  public static final byte KIND_F64 = 4;

  /** Layout kind {@code str}. */
  public static final byte KIND_STR = 5;

  /** Layout kind {@code date32} (days since the epoch). */
  public static final byte KIND_DATE32 = 6;

  /** Layout kind {@code time64_us} (microseconds since midnight). */
  public static final byte KIND_TIME64_US = 7;

  /** Layout kind {@code timestamp_us} (microseconds since the epoch, UTC). */
  public static final byte KIND_TIMESTAMP_US = 8;

  private final int layoutVersion;
  private final String[] layoutFields;
  private final byte[] layoutKinds;
  private final String[] topics;
  private final long[] timestampsUs;
  private final int[] rowOffsets;
  private final char[] fieldIndices;
  private final byte[] valueKinds;
  private final long[] valueBits;
  private final String[] strings;

  /**
   * Creates a batch; the arrays are adopted without copying.
   *
   * @param layoutVersion the version of this batch's layout
   * @param layoutFields the layout field names, or {@code null} when unchanged
   * @param layoutKinds the layout kinds (parallel to the fields), or {@code null} when unchanged
   * @param topics the consumer label of each row
   * @param timestampsUs the update time of each row, microseconds since the epoch
   * @param rowOffsets value position offsets, one more than the row count
   * @param fieldIndices the layout field of each value position
   * @param valueKinds the value kind of each position
   * @param valueBits the value payload of each position
   * @param strings the string values referenced by {@link #VALUE_STR} positions
   */
  public TickBatch(
      int layoutVersion,
      String[] layoutFields,
      byte[] layoutKinds,
      String[] topics,
      long[] timestampsUs,
      int[] rowOffsets,
      char[] fieldIndices,
      byte[] valueKinds,
      long[] valueBits,
      String[] strings) {
    this.layoutVersion = layoutVersion;
    this.layoutFields = layoutFields;
    this.layoutKinds = layoutKinds;
    this.topics = topics;
    this.timestampsUs = timestampsUs;
    this.rowOffsets = rowOffsets;
    this.fieldIndices = fieldIndices;
    this.valueKinds = valueKinds;
    this.valueBits = valueBits;
    this.strings = strings;
  }

  /**
   * Returns the layout version of every row.
   *
   * @return the version
   */
  public int layoutVersion() {
    return layoutVersion;
  }

  /**
   * Whether this batch carries its layout.
   *
   * @return {@code true} when {@link #layoutFields()} and {@link #layoutKinds()} are present
   */
  public boolean hasLayout() {
    return layoutFields != null;
  }

  /**
   * Returns a copy of the layout field names (a batch carries them only when the layout changed).
   *
   * @return the names, or {@code null} when the layout is unchanged
   */
  public String[] layoutFields() {
    return layoutFields == null ? null : layoutFields.clone();
  }

  /**
   * Returns a copy of the layout kinds (a batch carries them only when the layout changed).
   *
   * @return the kinds, or {@code null} when the layout is unchanged
   */
  public byte[] layoutKinds() {
    return layoutKinds == null ? null : layoutKinds.clone();
  }

  /**
   * Returns the number of rows.
   *
   * @return the row count
   */
  public int rowCount() {
    return topics.length;
  }

  /**
   * Returns the consumer label of a row.
   *
   * @param row the row
   * @return the label
   */
  public String topic(int row) {
    return topics[row];
  }

  /**
   * Returns the update time of a row.
   *
   * @param row the row
   * @return microseconds since the epoch
   */
  public long timestampUs(int row) {
    return timestampsUs[row];
  }

  /**
   * Returns the first value position of a row.
   *
   * @param row the row
   * @return the position
   */
  public int rowStart(int row) {
    return rowOffsets[row];
  }

  /**
   * Returns the end (exclusive) value position of a row.
   *
   * @param row the row
   * @return the position
   */
  public int rowEnd(int row) {
    return rowOffsets[row + 1];
  }

  /**
   * Returns the layout field of a value position.
   *
   * @param position the position
   * @return the field index
   */
  public int fieldIndex(int position) {
    return fieldIndices[position];
  }

  /**
   * Returns the value kind of a position.
   *
   * @param position the position
   * @return one of the {@code VALUE_*} constants
   */
  public byte valueKind(int position) {
    return valueKinds[position];
  }

  /**
   * Returns the raw value payload of a position.
   *
   * @param position the position
   * @return the bits
   */
  public long valueBits(int position) {
    return valueBits[position];
  }

  /**
   * Returns a string value.
   *
   * @param index the string index from {@link #valueBits(int)}
   * @return the string
   */
  public String string(int index) {
    return strings[index];
  }

  /**
   * Checks that the arrays agree with each other.
   *
   * @throws IllegalStateException if the batch is internally inconsistent
   */
  public void validate() {
    int rows = topics.length;
    if (timestampsUs.length != rows || rowOffsets.length != rows + 1) {
      throw new IllegalStateException("subscription batch rows are incomplete");
    }
    int positions = fieldIndices.length;
    if (valueKinds.length != positions || valueBits.length != positions) {
      throw new IllegalStateException("subscription batch values are incomplete");
    }
    if ((layoutFields == null) != (layoutKinds == null)
        || (layoutFields != null && layoutFields.length != layoutKinds.length)) {
      throw new IllegalStateException("subscription batch layout is incomplete");
    }
    int previous = 0;
    for (int offset : rowOffsets) {
      if (offset < previous || offset > positions) {
        throw new IllegalStateException("subscription batch row offsets are out of order");
      }
      previous = offset;
    }
    if (rows > 0 && (rowOffsets[0] != 0 || rowOffsets[rows] != positions)) {
      throw new IllegalStateException("subscription batch row offsets do not cover the values");
    }
    for (int position = 0; position < positions; position++) {
      byte kind = valueKinds[position];
      if (kind < VALUE_NULL || kind > VALUE_I64) {
        throw new IllegalStateException("subscription batch value kind " + kind + " is unknown");
      }
      long bits = valueBits[position];
      if (kind == VALUE_STR && (bits < 0 || bits >= strings.length)) {
        throw new IllegalStateException("subscription batch string index is out of range");
      }
    }
  }
}
