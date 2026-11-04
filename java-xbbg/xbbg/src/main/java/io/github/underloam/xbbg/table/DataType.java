package io.github.underloam.xbbg.table;

/**
 * Physical type of a {@link Column}, one per supported Arrow type. The code is the byte used by the
 * native transport.
 */
public enum DataType {
  /** All values null. */
  NULL(0, "null", 0),
  /** Boolean, bit-packed. */
  BOOL(1, "bool", 0),
  /** Signed 8-bit integer. */
  INT8(2, "int8", 1),
  /** Signed 16-bit integer. */
  INT16(3, "int16", 2),
  /** Signed 32-bit integer. */
  INT32(4, "int32", 4),
  /** Signed 64-bit integer. */
  INT64(5, "int64", 8),
  /** Unsigned 8-bit integer. */
  UINT8(6, "uint8", 1),
  /** Unsigned 16-bit integer. */
  UINT16(7, "uint16", 2),
  /** Unsigned 32-bit integer. */
  UINT32(8, "uint32", 4),
  /** Unsigned 64-bit integer. */
  UINT64(9, "uint64", 8),
  /** 32-bit float. */
  FLOAT32(10, "float32", 4),
  /** 64-bit float. */
  FLOAT64(11, "float64", 8),
  /** UTF-8 text with 32-bit offsets. */
  UTF8(12, "utf8", -1),
  /** UTF-8 text with 64-bit offsets. */
  LARGE_UTF8(13, "large_utf8", -1),
  /** Bytes with 32-bit offsets. */
  BINARY(14, "binary", -1),
  /** Bytes with 64-bit offsets. */
  LARGE_BINARY(15, "large_binary", -1),
  /** Days since 1970-01-01, 32-bit. */
  DATE32(16, "date32", 4),
  /** Milliseconds since 1970-01-01, 64-bit. */
  DATE64(17, "date64", 8),
  /** Seconds since midnight, 32-bit. */
  TIME32_S(18, "time32_s", 4),
  /** Milliseconds since midnight, 32-bit. */
  TIME32_MS(19, "time32_ms", 4),
  /** Microseconds since midnight, 64-bit. */
  TIME64_US(20, "time64_us", 8),
  /** Nanoseconds since midnight, 64-bit. */
  TIME64_NS(21, "time64_ns", 8),
  /** Seconds since the epoch. */
  TIMESTAMP_S(22, "timestamp_s", 8),
  /** Milliseconds since the epoch. */
  TIMESTAMP_MS(23, "timestamp_ms", 8),
  /** Microseconds since the epoch. */
  TIMESTAMP_US(24, "timestamp_us", 8),
  /** Nanoseconds since the epoch. */
  TIMESTAMP_NS(25, "timestamp_ns", 8);

  private static final DataType[] BY_CODE = values();

  private final int code;
  private final String typeName;
  private final int byteWidth;

  DataType(int code, String typeName, int byteWidth) {
    this.code = code;
    this.typeName = typeName;
    this.byteWidth = byteWidth;
  }

  /**
   * Returns the transport code.
   *
   * @return the code, 0 to 25
   */
  public int code() {
    return code;
  }

  /**
   * Returns the type name used in xbbg messages (for example {@code timestamp_us}).
   *
   * @return the type name
   */
  public String typeName() {
    return typeName;
  }

  /**
   * Returns the width of one value for fixed-width types.
   *
   * @return bytes per value; 0 for {@link #NULL} and {@link #BOOL}; -1 for variable-width types
   */
  public int byteWidth() {
    return byteWidth;
  }

  /**
   * Whether values are stored as offsets into a data buffer (text and binary).
   *
   * @return {@code true} for the UTF-8 and binary types
   */
  public boolean isVariableWidth() {
    return byteWidth < 0;
  }

  /**
   * Whether the offsets are 64-bit.
   *
   * @return {@code true} for {@link #LARGE_UTF8} and {@link #LARGE_BINARY}
   */
  public boolean hasLargeOffsets() {
    return this == LARGE_UTF8 || this == LARGE_BINARY;
  }

  /**
   * Whether this is a signed or unsigned integer type.
   *
   * @return {@code true} for {@code INT*} and {@code UINT*}
   */
  public boolean isInteger() {
    return code >= INT8.code && code <= UINT64.code;
  }

  /**
   * Whether this is a floating-point type.
   *
   * @return {@code true} for {@link #FLOAT32} and {@link #FLOAT64}
   */
  public boolean isFloatingPoint() {
    return this == FLOAT32 || this == FLOAT64;
  }

  /**
   * Whether this is a text type.
   *
   * @return {@code true} for {@link #UTF8} and {@link #LARGE_UTF8}
   */
  public boolean isText() {
    return this == UTF8 || this == LARGE_UTF8;
  }

  /**
   * Whether this is a binary type.
   *
   * @return {@code true} for {@link #BINARY} and {@link #LARGE_BINARY}
   */
  public boolean isBinary() {
    return this == BINARY || this == LARGE_BINARY;
  }

  /**
   * Whether this is a date type.
   *
   * @return {@code true} for {@link #DATE32} and {@link #DATE64}
   */
  public boolean isDate() {
    return this == DATE32 || this == DATE64;
  }

  /**
   * Whether this is a time-of-day type.
   *
   * @return {@code true} for the {@code TIME32_*} and {@code TIME64_*} types
   */
  public boolean isTime() {
    return code >= TIME32_S.code && code <= TIME64_NS.code;
  }

  /**
   * Whether this is a timestamp type.
   *
   * @return {@code true} for the {@code TIMESTAMP_*} types
   */
  public boolean isTimestamp() {
    return code >= TIMESTAMP_S.code && code <= TIMESTAMP_NS.code;
  }

  /**
   * Looks a type up by its transport code.
   *
   * @param code the code
   * @return the type
   * @throws IllegalArgumentException if no type has this code
   */
  public static DataType fromCode(int code) {
    if (code >= 0 && code < BY_CODE.length) {
      return BY_CODE[code];
    }
    throw new IllegalArgumentException("unknown data type code " + code);
  }

  @Override
  public String toString() {
    return typeName;
  }
}
