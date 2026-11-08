package io.github.underloam.xbbg.jdk17;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.table.Table;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;

class ArrowTablesTest {
  @Test
  void copiesSchemaValuesAndNulls() {
    Table table = TestTables.mixed();
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = ArrowTables.toVectorSchemaRoot(table, allocator)) {
      assertEquals(TestTables.ROWS, root.getRowCount());
      assertEquals(
          table.columnNames(), root.getSchema().getFields().stream().map(Field::getName).toList());
      assertEquals(
          Map.of("xbbg.request", "{\"id\":1}", "source", "test"),
          root.getSchema().getCustomMetadata());

      VarCharVector ticker = (VarCharVector) root.getVector("ticker");
      assertEquals("IBM US Equity", ticker.getObject(0).toString());
      assertTrue(ticker.isNull(1));
      assertEquals("AAPL US Equity", ticker.getObject(2).toString());

      Float8Vector px = (Float8Vector) root.getVector("px");
      assertEquals(101.5, px.get(0));
      assertEquals(2.25, px.get(1));
      assertTrue(px.isNull(2));

      BigIntVector volume = (BigIntVector) root.getVector("volume");
      assertFalse(volume.getField().isNullable());
      assertEquals(Long.MAX_VALUE, volume.get(2));
      assertEquals(-2, volume.get(1));

      TinyIntVector small = (TinyIntVector) root.getVector("small");
      assertEquals(-128, small.get(0));
      assertEquals(127, small.get(2));

      BitVector flag = (BitVector) root.getVector("flag");
      assertEquals(1, flag.get(0));
      assertEquals(0, flag.get(1));
      assertTrue(flag.isNull(2));

      UInt8Vector u64 = (UInt8Vector) root.getVector("u64");
      assertEquals(new ArrowType.Int(64, false), u64.getField().getType());
      assertEquals("18446744073709551615", Long.toUnsignedString(u64.get(0)));
      assertEquals(5, u64.get(2));

      UInt2Vector u16 = (UInt2Vector) root.getVector("u16");
      assertEquals(65535, u16.get(0));
      assertTrue(u16.isNull(2));

      DateDayVector date = (DateDayVector) root.getVector("date");
      assertEquals(19_000, date.get(0));
      assertTrue(date.isNull(1));

      TimeStampMicroTZVector ts = (TimeStampMicroTZVector) root.getVector("ts");
      assertEquals("America/New_York", ts.getTimeZone());
      assertEquals(1_700_000_000_000_001L, ts.get(0));
      assertEquals(-1L, ts.get(1));

      TimeMicroVector time = (TimeMicroVector) root.getVector("time");
      assertEquals(57_600_000_000L, time.get(1));
      assertTrue(time.isNull(2));

      LargeVarBinaryVector bytes = (LargeVarBinaryVector) root.getVector("bytes");
      assertArrayEquals(new byte[] {1, 2}, bytes.get(0));
      assertArrayEquals(new byte[0], bytes.get(1));
      assertTrue(bytes.isNull(2));

      FieldVector empty = root.getVector("empty");
      assertTrue(empty instanceof NullVector);
      assertEquals(TestTables.ROWS, empty.getValueCount());

      Float4Vector f32 = (Float4Vector) root.getVector("f32");
      assertEquals(-0.25f, f32.get(1));
      assertTrue(Float.isNaN(f32.get(2)));

      TimeStampMilliVector naive = (TimeStampMilliVector) root.getVector("tsNaive");
      assertEquals(3_000, naive.get(2));
    }
  }

  @Test
  void emptyTableHasSchemaOnly() {
    Table table =
        Table.fromArrowBuffers(
            new String[] {"ticker"},
            new byte[] {12},
            null,
            new boolean[] {true},
            0,
            new int[] {0},
            new byte[][] {new byte[0]},
            new byte[][] {TestTables.ints(0)},
            null,
            new String[0],
            new String[0],
            null);
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = ArrowTables.toVectorSchemaRoot(table, allocator)) {
      assertEquals(0, root.getRowCount());
      assertEquals(ArrowType.Utf8.INSTANCE, root.getVector("ticker").getField().getType());
    }
  }
}
