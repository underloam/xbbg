package io.github.underloam.xbbg.jdk17;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.Backend;
import io.github.underloam.xbbg.internal.Backends;
import io.github.underloam.xbbg.internal.ResultKind;
import io.github.underloam.xbbg.table.Table;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compare.VectorEqualsVisitor;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

class ArrowBackendsTest {
  private static final Schema SCHEMA =
      new Schema(
          List.of(
              Field.nullable("ticker", ArrowType.Utf8.INSTANCE),
              Field.nullable("px", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
              new Field(
                  "time",
                  new FieldType(false, new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), null),
                  null)),
          Map.of("xbbg.request", "{\"id\":7}"));

  private static Consumer<VectorSchemaRoot> batch(String[] tickers, Double[] prices, long[] times) {
    return root -> {
      root.allocateNew();
      VarCharVector ticker = (VarCharVector) root.getVector("ticker");
      Float8Vector px = (Float8Vector) root.getVector("px");
      TimeStampMicroTZVector time = (TimeStampMicroTZVector) root.getVector("time");
      for (int i = 0; i < tickers.length; i++) {
        if (tickers[i] == null) {
          ticker.setNull(i);
        } else {
          ticker.setSafe(i, tickers[i].getBytes(StandardCharsets.UTF_8));
        }
        if (prices[i] == null) {
          px.setNull(i);
        } else {
          px.setSafe(i, prices[i]);
        }
        time.setSafe(i, times[i]);
      }
      root.setRowCount(tickers.length);
    };
  }

  @Test
  void backendReadsTheIpcStreamResultKind() {
    try (BufferAllocator allocator = new RootAllocator()) {
      Backend<VectorSchemaRoot> backend = ArrowBackends.vectorSchemaRoot(allocator);
      assertEquals(ResultKind.IPC_STREAM, Backends.nativeKind(backend));
      assertEquals("vectorSchemaRoot", backend.name());
    }
  }

  @Test
  void concatenatesEveryBatchIntoOneCallerOwnedRoot() {
    try (BufferAllocator allocator = new RootAllocator()) {
      byte[] ipc;
      try (VectorSchemaRoot writer = VectorSchemaRoot.create(SCHEMA, allocator)) {
        ipc =
            TestTables.ipcStream(
                writer,
                List.of(
                    batch(
                        new String[] {"IBM US Equity", null},
                        new Double[] {101.5, null},
                        new long[] {1, 2}),
                    batch(
                        new String[] {"AAPL US Equity", "MSFT US Equity", "SPY US Equity"},
                        new Double[] {null, 3.0, 4.5},
                        new long[] {3, 4, 5})));
      }
      assertEquals(0, allocator.getAllocatedMemory());

      VectorSchemaRoot root = Backends.fromNative(ArrowBackends.vectorSchemaRoot(allocator), ipc);
      try {
        assertEquals(SCHEMA, root.getSchema());
        assertEquals(5, root.getRowCount());
        assertEquals(
            "ticker\tpx\ttime\n"
                + "IBM US Equity\t101.5\t"
                + micros(1)
                + "\n"
                + "null\tnull\t"
                + micros(2)
                + "\n"
                + "AAPL US Equity\tnull\t"
                + micros(3)
                + "\n"
                + "MSFT US Equity\t3.0\t"
                + micros(4)
                + "\n"
                + "SPY US Equity\t4.5\t"
                + micros(5)
                + "\n",
            root.contentToTSVString());
        assertTrue(allocator.getAllocatedMemory() > 0, "the root holds the data");
      } finally {
        root.close();
      }
      // the reader's buffers were released; only the caller's root held memory
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void streamWithoutBatchesGivesAnEmptyRoot() {
    try (BufferAllocator allocator = new RootAllocator()) {
      byte[] ipc;
      try (VectorSchemaRoot writer = VectorSchemaRoot.create(SCHEMA, allocator)) {
        ipc = TestTables.ipcStream(writer, List.of());
      }
      try (VectorSchemaRoot root =
          Backends.fromNative(ArrowBackends.vectorSchemaRoot(allocator), ipc)) {
        assertEquals(SCHEMA, root.getSchema());
        assertEquals(0, root.getRowCount());
      }
    }
  }

  @Test
  void malformedStreamFailsWithoutLeaking() {
    try (BufferAllocator allocator = new RootAllocator()) {
      Backend<VectorSchemaRoot> backend = ArrowBackends.vectorSchemaRoot(allocator);
      assertThrows(
          UncheckedIOException.class, () -> Backends.fromNative(backend, new byte[] {1, 2, 3}));
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void tableRoundTripsThroughIpcAndTheBackend() {
    Table table = TestTables.mixed();
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot expected = ArrowTables.toVectorSchemaRoot(table, allocator)) {
      byte[] ipc = TestTables.ipcStream(expected);
      try (VectorSchemaRoot actual =
          Backends.fromNative(ArrowBackends.vectorSchemaRoot(allocator), ipc)) {
        assertEquals(expected.getSchema(), actual.getSchema());
        assertEquals(TestTables.ROWS, actual.getRowCount());
        for (FieldVector vector : expected.getFieldVectors()) {
          String name = vector.getName();
          assertTrue(
              VectorEqualsVisitor.vectorEquals(vector, actual.getVector(name)),
              () -> name + " differs after the IPC round trip");
        }
        assertArrayEquals(
            new byte[] {1, 2}, ((LargeVarBinaryVector) actual.getVector("bytes")).get(0));
      }
    }
  }

  @Test
  void rejectsNullAllocator() {
    assertThrows(NullPointerException.class, () -> ArrowBackends.vectorSchemaRoot(null));
    Table table = TestTables.mixed();
    assertThrows(NullPointerException.class, () -> ArrowTables.toVectorSchemaRoot(table, null));
    try (BufferAllocator allocator = new RootAllocator()) {
      assertThrows(
          NullPointerException.class, () -> ArrowTables.toVectorSchemaRoot(null, allocator));
    }
  }

  private static String micros(long micros) {
    // TimeStampMicroTZVector.getObject is the raw long
    return Long.toString(micros);
  }
}
