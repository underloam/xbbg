package io.github.underloam.xbbg.table;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.BlpInternalException;
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
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

class TableTest {
  /** Collects canonical column buffers for {@link Table#fromArrowBuffers}. */
  static final class Batch {
    final int rows;
    final List<String> names = new ArrayList<String>();
    final List<Byte> types = new ArrayList<Byte>();
    final List<String> zones = new ArrayList<String>();
    final List<Boolean> nullable = new ArrayList<Boolean>();
    final List<Integer> nulls = new ArrayList<Integer>();
    final List<byte[]> values = new ArrayList<byte[]>();
    final List<byte[]> offsets = new ArrayList<byte[]>();
    final List<byte[]> validity = new ArrayList<byte[]>();

    Batch(int rows) {
      this.rows = rows;
    }

    Batch add(String name, DataType type, String zone, byte[] data, byte[] offs, boolean[] valid) {
      int nullCount = 0;
      if (valid != null) {
        for (boolean v : valid) {
          nullCount += v ? 0 : 1;
        }
      }
      names.add(name);
      types.add((byte) type.code());
      zones.add(zone);
      nullable.add(valid != null);
      nulls.add(nullCount);
      values.add(data);
      offsets.add(offs);
      validity.add(nullCount == 0 ? null : bits(valid));
      return this;
    }

    Table build(Map<String, String> metadata, String decoded) {
      int n = names.size();
      boolean[] flags = new boolean[n];
      int[] counts = new int[n];
      byte[] codes = new byte[n];
      for (int i = 0; i < n; i++) {
        flags[i] = nullable.get(i);
        counts[i] = nulls.get(i);
        codes[i] = types.get(i);
      }
      return Table.fromArrowBuffers(
          names.toArray(new String[n]),
          codes,
          zones.toArray(new String[n]),
          flags,
          rows,
          counts,
          values.toArray(new byte[n][]),
          offsets.toArray(new byte[n][]),
          validity.toArray(new byte[n][]),
          metadata.keySet().toArray(new String[0]),
          metadata.values().toArray(new String[0]),
          decoded);
    }
  }

  static byte[] bits(boolean... values) {
    byte[] out = new byte[(values.length + 7) / 8];
    for (int i = 0; i < values.length; i++) {
      if (values[i]) {
        out[i >>> 3] |= (byte) (1 << (i & 7));
      }
    }
    return out;
  }

  static byte[] ints(int... values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * 4).order(ByteOrder.LITTLE_ENDIAN);
    for (int value : values) {
      buffer.putInt(value);
    }
    return buffer.array();
  }

  static byte[] longs(long... values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * 8).order(ByteOrder.LITTLE_ENDIAN);
    for (long value : values) {
      buffer.putLong(value);
    }
    return buffer.array();
  }

  static byte[] shorts(int... values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * 2).order(ByteOrder.LITTLE_ENDIAN);
    for (int value : values) {
      buffer.putShort((short) value);
    }
    return buffer.array();
  }

  static byte[] doubles(double... values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * 8).order(ByteOrder.LITTLE_ENDIAN);
    for (double value : values) {
      buffer.putDouble(value);
    }
    return buffer.array();
  }

  static byte[] floats(float... values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * 4).order(ByteOrder.LITTLE_ENDIAN);
    for (float value : values) {
      buffer.putFloat(value);
    }
    return buffer.array();
  }

  static byte[] utf8(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }

  private static final boolean[] MIDDLE_NULL = {true, false, true};

  private static Table everyType() {
    long day = 19739; // 2024-01-17
    Batch batch =
        new Batch(3)
            .add("null", DataType.NULL, null, null, null, null)
            .add("bool", DataType.BOOL, null, bits(true, false, false), null, MIDDLE_NULL)
            .add("int8", DataType.INT8, null, new byte[] {-128, 0, 127}, null, null)
            .add("int16", DataType.INT16, null, shorts(-32768, 0, 32767), null, null)
            .add("int32", DataType.INT32, null, ints(Integer.MIN_VALUE, 0, 7), null, MIDDLE_NULL)
            .add(
                "int64", DataType.INT64, null, longs(Long.MIN_VALUE, 0, Long.MAX_VALUE), null, null)
            .add("uint8", DataType.UINT8, null, new byte[] {(byte) 255, 0, 1}, null, null)
            .add("uint16", DataType.UINT16, null, shorts(65535, 0, 1), null, null)
            .add("uint32", DataType.UINT32, null, ints(-1, 0, 1), null, null)
            .add("uint64", DataType.UINT64, null, longs(-1L, 0, 1), null, null)
            .add("float32", DataType.FLOAT32, null, floats(1.5f, 0f, -2.25f), null, null)
            .add(
                "float64",
                DataType.FLOAT64,
                null,
                doubles(1.5, Double.NaN, -0.0),
                null,
                MIDDLE_NULL)
            .add("utf8", DataType.UTF8, null, utf8("abch\u00e9llo"), ints(0, 3, 3, 9), MIDDLE_NULL)
            .add("large_utf8", DataType.LARGE_UTF8, null, utf8("xyz"), longs(0, 1, 1, 3), null)
            .add("binary", DataType.BINARY, null, new byte[] {1, 2, 3}, ints(0, 0, 2, 3), null)
            .add(
                "large_binary",
                DataType.LARGE_BINARY,
                null,
                new byte[] {9},
                longs(0, 1, 1, 1),
                null)
            .add("date32", DataType.DATE32, null, ints((int) day, 0, -1), null, null)
            .add(
                "date64",
                DataType.DATE64,
                null,
                longs(day * 86_400_000L, 0, -86_400_000L),
                null,
                null)
            .add("time32_s", DataType.TIME32_S, null, ints(3600, 0, 86399), null, null)
            .add("time32_ms", DataType.TIME32_MS, null, ints(3_600_500, 0, 1), null, null)
            .add("time64_us", DataType.TIME64_US, null, longs(3_600_000_001L, 0, 1), null, null)
            .add("time64_ns", DataType.TIME64_NS, null, longs(3_600_000_000_001L, 0, 1), null, null)
            .add(
                "timestamp_s",
                DataType.TIMESTAMP_S,
                "UTC",
                longs(1_705_500_000L, 0, -1),
                null,
                null)
            .add(
                "timestamp_ms",
                DataType.TIMESTAMP_MS,
                "+05:30",
                longs(1_705_500_000_123L, 0, -1),
                null,
                null)
            .add(
                "timestamp_us",
                DataType.TIMESTAMP_US,
                "America/New_York",
                longs(1_705_500_000_123_456L, 0, -1),
                null,
                MIDDLE_NULL)
            .add(
                "timestamp_ns",
                DataType.TIMESTAMP_NS,
                null,
                longs(1_705_500_000_123_456_789L, 0, -1),
                null,
                null);
    Map<String, String> metadata = new LinkedHashMap<String, String>();
    metadata.put("xbbg.bloomberg_request_id", "abc");
    return batch.build(metadata, null);
  }

  @Test
  void decodesEveryTransportType() {
    Table table = everyType();
    assertEquals(3, table.numRows());
    assertEquals(26, table.numColumns());
    for (Column column : table.columns()) {
      assertEquals(column.name(), column.type().typeName(), "column names match their types here");
    }

    assertTrue(table.column("null").isNull(0));
    assertEquals(3, table.column("null").nullCount());
    assertEquals(Arrays.asList(true, null, false), table.column("bool").toList());
    assertEquals(
        Arrays.<Object>asList((byte) -128, (byte) 0, (byte) 127), table.column("int8").toList());
    assertEquals(
        Arrays.<Object>asList((short) -32768, (short) 0, (short) 32767),
        table.column("int16").toList());
    assertEquals(Arrays.<Object>asList(Integer.MIN_VALUE, null, 7), table.column("int32").toList());
    assertEquals(
        Arrays.<Object>asList(Long.MIN_VALUE, 0L, Long.MAX_VALUE), table.column("int64").toList());
    assertEquals(
        Arrays.<Object>asList((short) 255, (short) 0, (short) 1), table.column("uint8").toList());
    assertEquals(Arrays.<Object>asList(65535, 0, 1), table.column("uint16").toList());
    assertEquals(Arrays.<Object>asList(4294967295L, 0L, 1L), table.column("uint32").toList());
    assertEquals(new BigInteger("18446744073709551615"), table.column("uint64").get(0));
    assertEquals(-1L, table.column("uint64").getLong(0));
    assertEquals(1.8446744073709552E19, table.column("uint64").getDouble(0));
    assertEquals(Arrays.<Object>asList(1.5f, 0f, -2.25f), table.column("float32").toList());
    assertEquals(1.5, table.column("float64").getDouble(0));
    assertNull(table.column("float64").get(1));
    assertEquals(Double.valueOf(-0.0), table.column("float64").get(2));
    assertEquals(Arrays.<Object>asList("abc", null, "h\u00e9llo"), table.column("utf8").toList());
    assertEquals(Arrays.<Object>asList("x", "", "yz"), table.column("large_utf8").toList());
    assertArrayEquals(new byte[0], table.column("binary").getBytes(0));
    assertArrayEquals(new byte[] {1, 2}, table.column("binary").getBytes(1));
    assertArrayEquals(new byte[] {9}, table.column("large_binary").getBytes(0));
    assertArrayEquals(utf8("abc"), table.column("utf8").getBytes(0));

    assertEquals(LocalDate.of(2024, 1, 17), table.column("date32").getLocalDate(0));
    assertEquals(LocalDate.of(1969, 12, 31), table.column("date32").get(2));
    assertEquals(LocalDate.of(2024, 1, 17), table.column("date64").getLocalDate(0));
    assertEquals(LocalDate.of(1969, 12, 31), table.column("date64").getLocalDate(2));
    assertEquals(LocalTime.of(1, 0), table.column("time32_s").getLocalTime(0));
    assertEquals(LocalTime.of(23, 59, 59), table.column("time32_s").get(2));
    assertEquals(LocalTime.of(1, 0, 0, 500_000_000), table.column("time32_ms").getLocalTime(0));
    assertEquals(LocalTime.of(1, 0, 0, 1_000), table.column("time64_us").getLocalTime(0));
    assertEquals(LocalTime.of(1, 0, 0, 1), table.column("time64_ns").getLocalTime(0));

    Column utc = table.column("timestamp_s");
    assertEquals(Instant.ofEpochSecond(1_705_500_000L), utc.get(0));
    assertEquals(Instant.ofEpochSecond(-1), utc.getInstant(2));
    Column offset = table.column("timestamp_ms");
    assertEquals("+05:30", offset.timezone().get());
    assertEquals(LocalDateTime.of(2024, 1, 17, 19, 30, 0, 123_000_000), offset.getLocalDateTime(0));
    Column newYork = table.column("timestamp_us");
    assertEquals(Instant.parse("2024-01-17T14:00:00.123456Z"), newYork.getInstant(0));
    assertEquals(
        ZonedDateTime.of(2024, 1, 17, 9, 0, 0, 123_456_000, ZoneId.of("America/New_York")),
        newYork.getZonedDateTime(0));
    assertNull(newYork.getZonedDateTime(1));
    assertEquals(Instant.parse("1969-12-31T23:59:59.999999Z"), newYork.getInstant(2));
    Column naive = table.column("timestamp_ns");
    assertFalse(naive.timezone().isPresent());
    assertEquals(LocalDateTime.of(2024, 1, 17, 14, 0, 0, 123_456_789), naive.get(0));
    assertEquals(Instant.parse("2024-01-17T14:00:00.123456789Z"), naive.getInstant(0));
    assertEquals(LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_999_999), naive.get(2));
  }

  @Test
  void rowsMirrorColumnValues() {
    Table table = everyType();
    Row row = table.row(2);
    assertEquals(2, row.index());
    assertEquals("h\u00e9llo", row.getString("utf8"));
    assertEquals(7, row.getInt("int32"));
    assertTrue(row.isNull("null"));
    Map<String, Object> map = row.toMap();
    assertEquals(26, map.size());
    assertEquals(table.columnNames(), new ArrayList<String>(map.keySet()));
    assertEquals(Long.MAX_VALUE, map.get("int64"));

    RowsResult rows = table.toRows();
    assertEquals(3, rows.size());
    assertEquals(table.columnNames(), rows.columnNames());
    assertNull(rows.get(1).get("utf8"));
    assertTrue(rows.get(1).containsKey("utf8"));
    assertEquals(Collections.singletonMap("xbbg.bloomberg_request_id", "abc"), rows.metadata());
    assertThrows(
        UnsupportedOperationException.class,
        () -> rows.add(Collections.<String, Object>emptyMap()));
    assertThrows(UnsupportedOperationException.class, () -> rows.get(0).put("x", 1));
  }

  @Test
  void gettersCheckTypesNullsAndBounds() {
    Table table = everyType();
    UnsupportedOperationException wrongType =
        assertThrows(UnsupportedOperationException.class, () -> table.column("int64").getString(0));
    assertEquals(
        "getString does not apply to column 'int64' of type int64", wrongType.getMessage());
    assertThrows(UnsupportedOperationException.class, () -> table.column("utf8").getLong(0));
    assertThrows(UnsupportedOperationException.class, () -> table.column("int64").getInt(0));
    assertThrows(UnsupportedOperationException.class, () -> table.column("bool").getDouble(0));
    IllegalStateException nullValue =
        assertThrows(IllegalStateException.class, () -> table.column("int32").getInt(1));
    assertEquals("column 'int32' is null at row 1", nullValue.getMessage());
    assertThrows(IndexOutOfBoundsException.class, () -> table.column("int32").get(3));
    assertThrows(IndexOutOfBoundsException.class, () -> table.row(-1));
    assertThrows(IllegalArgumentException.class, () -> table.column("missing"));
    assertFalse(table.findColumn("missing").isPresent());
  }

  @Test
  void zeroRowTablesAreValid() {
    Table table =
        new Batch(0)
            .add("ticker", DataType.UTF8, null, new byte[0], ints(0), null)
            .add("value", DataType.FLOAT64, null, new byte[0], null, null)
            .build(Collections.<String, String>emptyMap(), null);
    assertEquals(0, table.numRows());
    assertEquals(Arrays.asList("ticker", "value"), table.columnNames());
    assertFalse(table.iterator().hasNext());
    assertTrue(table.toRows().isEmpty());
    assertEquals(ResultMetadata.empty(), table.resultMetadata());
  }

  @Test
  void rejectsInconsistentBuffers() {
    assertInternal(
        "native Arrow column v data buffer is too small: expected at least 24 bytes, got 16",
        new Batch(3).add("v", DataType.INT64, null, longs(1, 2), null, null));
    assertInternal(
        "native Arrow column s is missing offsets buffer",
        new Batch(1).add("s", DataType.UTF8, null, utf8("a"), null, null));
    assertInternal(
        "native Arrow column s has invalid offset 5 at index 1",
        new Batch(1).add("s", DataType.UTF8, null, utf8("a"), ints(0, 5), null));
    assertInternal(
        "native Arrow column s has invalid offset 0 at index 2",
        new Batch(2).add("s", DataType.UTF8, null, utf8("ab"), ints(0, 2, 0), null));
    assertInternal(
        "native Arrow column v has a time zone but type int64",
        new Batch(1).add("v", DataType.INT64, "UTC", longs(1), null, null));
  }

  @Test
  void rejectsNullCountsThatDisagreeWithTheBitmap() {
    BlpInternalException error =
        assertThrows(
            BlpInternalException.class,
            () ->
                Table.fromArrowBuffers(
                    new String[] {"v"},
                    new byte[] {(byte) DataType.INT32.code()},
                    null,
                    new boolean[] {true},
                    3,
                    new int[] {2},
                    new byte[][] {ints(1, 2, 3)},
                    null,
                    new byte[][] {bits(true, false, true)},
                    null,
                    null,
                    null));
    assertEquals(
        "native Arrow column v declares 2 nulls but its validity bitmap has 1", error.getMessage());
  }

  @Test
  void rejectsUnsupportedTypeCodesListingEveryColumn() {
    BlpInternalException error =
        assertThrows(
            BlpInternalException.class,
            () ->
                Table.fromArrowBuffers(
                    new String[] {"ok", "bad1", "bad2"},
                    new byte[] {5, 26, (byte) 200},
                    null,
                    new boolean[3],
                    0,
                    new int[3],
                    new byte[3][],
                    new byte[3][],
                    new byte[3][],
                    null,
                    null,
                    null));
    assertEquals(
        "native Arrow batch has unsupported columns: #1 'bad1' (unsupported type code 26),"
            + " #2 'bad2' (unsupported type code 200)",
        error.getMessage());
    assertThrows(
        BlpInternalException.class,
        () ->
            Table.fromArrowBuffers(
                new String[] {"a"},
                new byte[0],
                null,
                new boolean[1],
                0,
                new int[1],
                null,
                null,
                null,
                null,
                null,
                null));
  }

  @Test
  void timesOutsideOneDayFailWithDateTimeException() {
    Table table =
        new Batch(3)
            .add(
                "t",
                DataType.TIME64_US,
                null,
                longs(Long.MAX_VALUE, -1, 86_399_999_999L),
                null,
                null)
            .build(new LinkedHashMap<String, String>(), null);
    Column column = table.column("t");
    DateTimeException overflow =
        assertThrows(DateTimeException.class, () -> column.getLocalTime(0));
    assertEquals(
        "column 't' row 0 holds a time outside one day: " + Long.MAX_VALUE + " us",
        overflow.getMessage());
    assertThrows(DateTimeException.class, () -> column.get(1));
    assertEquals(LocalTime.of(23, 59, 59, 999_999_000), column.getLocalTime(2));
  }

  @Test
  void decodesEngineMetadataLazily() {
    String decoded =
        "{\"bloombergRequestId\":\"rid\",\"bloombergRequestIds\":[\"rid\"],"
            + "\"eidData\":{\"IBM US Equity\":[14005,35009]},"
            + "\"delayed\":{\"IBM US Equity\":{\"delayed\":true,\"delayMinutes\":15},"
            + "\"MSFT US Equity\":{\"delayed\":false,\"delayMinutes\":null}},"
            + "\"securityErrors\":{\"BAD Equity\":{\"category\":\"BAD_SEC\",\"code\":\"7\","
            + "\"subcategory\":\"INVALID_SECURITY\",\"message\":\"Unknown/Invalid security\"}},"
            + "\"fieldExceptions\":{\"IBM US Equity\":[{\"field\":\"BAD_FLD\",\"category\":\"BAD_FLD\","
            + "\"code\":9,\"subcategory\":\"NOT_APPLICABLE_TO_REF_DATA\",\"message\":\"Field not applicable\"}]},"
            + "\"fieldErrors\":{\"XX\":{\"category\":\"BAD_FLD\",\"code\":10,\"subcategory\":\"INVALID_FIELD\","
            + "\"message\":\"Unknown field\",\"source\":\"apiflds\"}},"
            + "\"typeMisses\":{\"PX_LAST\":2}}";
    Map<String, String> raw = new LinkedHashMap<String, String>();
    raw.put("xbbg.bloomberg_request_id", "rid");
    Table table =
        new Batch(1)
            .add("ticker", DataType.UTF8, null, utf8("I"), ints(0, 1), null)
            .build(raw, decoded);
    assertEquals(raw, table.metadata());
    assertEquals("rid", table.bloombergRequestId().get());
    assertEquals(Collections.singletonList("rid"), table.bloombergRequestIds());
    assertEquals(Arrays.asList(14005, 35009), table.eidData().get().get("IBM US Equity"));
    DelayedDataInfo ibm = table.delayed().get().get("IBM US Equity");
    assertTrue(ibm.delayed());
    assertEquals(OptionalInt.of(15), ibm.delayMinutes());
    assertFalse(table.delayed().get().get("MSFT US Equity").delayMinutes().isPresent());
    assertEquals(
        new BloombergMetadataError("BAD_SEC", "7", "INVALID_SECURITY", "Unknown/Invalid security"),
        table.securityErrors().get().get("BAD Equity"));
    assertEquals(
        Collections.singletonList(
            new BloombergFieldException(
                "BAD_FLD", "BAD_FLD", "9", "NOT_APPLICABLE_TO_REF_DATA", "Field not applicable")),
        table.fieldExceptions().get().get("IBM US Equity"));
    assertEquals(
        new BloombergFieldError("BAD_FLD", 10, "INVALID_FIELD", "Unknown field", "apiflds"),
        table.fieldErrors().get().get("XX"));
    assertEquals(Long.valueOf(2), table.typeMisses().get().get("PX_LAST"));
    assertEquals(table.resultMetadata(), table.toRows().resultMetadata());
  }

  @Test
  void malformedFieldMetadataFailsOnlyItsAccessor() {
    String decoded =
        "{\"fieldExceptionsError\":\"Invalid xbbg.field_exceptions metadata: expected valid JSON\","
            + "\"fieldErrorsError\":\"Invalid xbbg.field_errors metadata: malformed field errors\","
            + "\"typeMisses\":{\"A\":1}}";
    Table table =
        new Batch(0)
            .add("ticker", DataType.UTF8, null, new byte[0], ints(0), null)
            .build(Collections.<String, String>emptyMap(), decoded);
    BlpInternalException exceptions =
        assertThrows(BlpInternalException.class, table::fieldExceptions);
    assertEquals(
        "Invalid xbbg.field_exceptions metadata: expected valid JSON", exceptions.getMessage());
    BlpInternalException errors = assertThrows(BlpInternalException.class, table::fieldErrors);
    assertEquals("Invalid xbbg.field_errors metadata: malformed field errors", errors.getMessage());
    assertEquals(Long.valueOf(1), table.typeMisses().get().get("A"));
    assertFalse(table.delayed().isPresent());
  }

  @Test
  void malformedDecodedMetadataIsAnInternalError() {
    Table table =
        new Batch(0)
            .add("t", DataType.UTF8, null, new byte[0], ints(0), null)
            .build(Collections.<String, String>emptyMap(), "{\"delayed\":[]}");
    BlpInternalException error = assertThrows(BlpInternalException.class, table::resultMetadata);
    assertEquals("malformed native JSON: delayed must be an object", error.getMessage());
  }

  private static void assertInternal(String message, Batch batch) {
    BlpInternalException error =
        assertThrows(
            BlpInternalException.class,
            () -> batch.build(Collections.<String, String>emptyMap(), null));
    assertEquals(message, error.getMessage());
  }
}
