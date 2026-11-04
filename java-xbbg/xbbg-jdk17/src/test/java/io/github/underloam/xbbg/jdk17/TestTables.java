package io.github.underloam.xbbg.jdk17;

import io.github.underloam.xbbg.table.DataType;
import io.github.underloam.xbbg.table.Table;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

/** Builds xbbg tables from canonical Arrow buffers and writes Arrow IPC streams. */
final class TestTables {
  static final int ROWS = 3;

  private TestTables() {}

  /**
   * A three-row table with one column of every xbbg type family, nulls in most columns and schema
   * metadata.
   */
  static Table mixed() {
    String[] names = {
      "ticker", "px", "volume", "small", "flag", "u64", "u16", "date", "ts", "time", "bytes",
      "empty", "f32", "tsNaive"
    };
    DataType[] types = {
      DataType.UTF8,
      DataType.FLOAT64,
      DataType.INT64,
      DataType.INT8,
      DataType.BOOL,
      DataType.UINT64,
      DataType.UINT16,
      DataType.DATE32,
      DataType.TIMESTAMP_US,
      DataType.TIME64_US,
      DataType.LARGE_BINARY,
      DataType.NULL,
      DataType.FLOAT32,
      DataType.TIMESTAMP_MS
    };
    byte[] codes = new byte[types.length];
    for (int i = 0; i < types.length; i++) {
      codes[i] = (byte) types[i].code();
    }
    String[] zones = new String[types.length];
    zones[8] = "America/New_York";
    boolean[] nullable = new boolean[types.length];
    Arrays.fill(nullable, true);
    nullable[2] = false;
    byte[][] values = new byte[types.length][];
    byte[][] offsets = new byte[types.length][];
    byte[][] validity = new byte[types.length][];
    int[] nullCounts = new int[types.length];

    // ticker: "IBM US Equity", null, "AAPL US Equity"
    byte[] ibm = "IBM US Equity".getBytes(StandardCharsets.UTF_8);
    byte[] aapl = "AAPL US Equity".getBytes(StandardCharsets.UTF_8);
    values[0] = concat(ibm, aapl);
    offsets[0] = ints(0, ibm.length, ibm.length, ibm.length + aapl.length);
    validity[0] = valid(true, false, true);
    nullCounts[0] = 1;
    // px: 101.5, 2.25, null
    values[1] = doubles(101.5, 2.25, 0);
    validity[1] = valid(true, true, false);
    nullCounts[1] = 1;
    // volume (not nullable): 1, -2, Long.MAX_VALUE
    values[2] = longs(1, -2, Long.MAX_VALUE);
    // small: -128, 0, 127
    values[3] = new byte[] {-128, 0, 127};
    // flag: true, false, null
    values[4] = new byte[] {0b001};
    validity[4] = valid(true, true, false);
    nullCounts[4] = 1;
    // u64: 2^64 - 1, 0, 5
    values[5] = longs(-1L, 0, 5);
    // u16: 65535, 1, null
    values[6] = shorts((short) -1, (short) 1, (short) 0);
    validity[6] = valid(true, true, false);
    nullCounts[6] = 1;
    // date: 2022-01-08, null, 1970-01-01
    values[7] = ints(19_000, 0, 0);
    validity[7] = valid(true, false, true);
    nullCounts[7] = 1;
    // ts (zoned, us)
    values[8] = longs(1_700_000_000_000_001L, -1L, 0);
    // time: 09:30, 16:00, null
    values[9] = longs(34_200_000_000L, 57_600_000_000L, 0);
    validity[9] = valid(true, true, false);
    nullCounts[9] = 1;
    // bytes: {1,2}, {}, null (64-bit offsets)
    values[10] = new byte[] {1, 2};
    offsets[10] = longs(0, 2, 2, 2);
    validity[10] = valid(true, true, false);
    nullCounts[10] = 1;
    // empty: NULL column
    nullCounts[11] = ROWS;
    // f32
    values[12] = floats(1.5f, -0.25f, Float.NaN);
    // tsNaive (ms, no zone)
    values[13] = longs(1_000, 2_000, 3_000);

    return Table.fromArrowBuffers(
        names,
        codes,
        zones,
        nullable,
        ROWS,
        nullCounts,
        values,
        offsets,
        validity,
        new String[] {"xbbg.request", "source"},
        new String[] {"{\"id\":1}", "test"},
        null);
  }

  /** Writes a root's current contents as a one-batch IPC stream. */
  static byte[] ipcStream(VectorSchemaRoot root) {
    return ipcStream(root, List.of(filled -> {}));
  }

  /**
   * Writes an IPC stream with one record batch per filler; each filler sets the root's contents
   * before its batch is written.
   */
  static byte[] ipcStream(VectorSchemaRoot root, List<Consumer<VectorSchemaRoot>> batches) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
      writer.start();
      for (Consumer<VectorSchemaRoot> batch : batches) {
        batch.accept(root);
        writer.writeBatch();
      }
      writer.end();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return out.toByteArray();
  }

  private static byte[] concat(byte[] a, byte[] b) {
    byte[] out = new byte[a.length + b.length];
    System.arraycopy(a, 0, out, 0, a.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }

  private static ByteBuffer le(byte[] bytes) {
    return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
  }

  static byte[] longs(long... values) {
    byte[] bytes = new byte[values.length * 8];
    ByteBuffer out = le(bytes);
    for (long value : values) {
      out.putLong(value);
    }
    return bytes;
  }

  static byte[] ints(int... values) {
    byte[] bytes = new byte[values.length * 4];
    ByteBuffer out = le(bytes);
    for (int value : values) {
      out.putInt(value);
    }
    return bytes;
  }

  static byte[] shorts(short... values) {
    byte[] bytes = new byte[values.length * 2];
    ByteBuffer out = le(bytes);
    for (short value : values) {
      out.putShort(value);
    }
    return bytes;
  }

  static byte[] doubles(double... values) {
    byte[] bytes = new byte[values.length * 8];
    ByteBuffer out = le(bytes);
    for (double value : values) {
      out.putDouble(value);
    }
    return bytes;
  }

  static byte[] floats(float... values) {
    byte[] bytes = new byte[values.length * 4];
    ByteBuffer out = le(bytes);
    for (float value : values) {
      out.putFloat(value);
    }
    return bytes;
  }

  static byte[] valid(boolean... rows) {
    byte[] out = new byte[(rows.length + 7) / 8];
    for (int i = 0; i < rows.length; i++) {
      if (rows[i]) {
        out[i >>> 3] |= (byte) (1 << (i & 7));
      }
    }
    return out;
  }
}
