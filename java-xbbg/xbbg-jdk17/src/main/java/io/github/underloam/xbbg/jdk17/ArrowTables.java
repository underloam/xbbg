package io.github.underloam.xbbg.jdk17;

import io.github.underloam.xbbg.table.Column;
import io.github.underloam.xbbg.table.DataType;
import io.github.underloam.xbbg.table.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Copies xbbg {@link Table}s into Arrow Java vectors. Requires {@code arrow-vector} and an
 * allocator implementation on the class path (see {@link ArrowBackends}).
 *
 * <p>To receive Arrow vectors from a request without building a {@link Table} first, pass {@link
 * ArrowBackends#vectorSchemaRoot} as the backend instead; it reads the engine's IPC stream
 * directly.
 */
public final class ArrowTables {
  private ArrowTables() {}

  /**
   * Copies a table into a new {@link VectorSchemaRoot}: same column names, Arrow types, timestamp
   * zones, nullability and schema metadata, and the same values and nulls. Unsigned integers keep
   * their bit patterns (Arrow's {@code UInt*} vectors); times, dates and timestamps keep their raw
   * values in their unit.
   *
   * <p>The caller owns the returned root and must {@link VectorSchemaRoot#close() close} it.
   *
   * @param table the table
   * @param allocator the allocator for the vectors
   * @return a root owned by the caller
   */
  public static VectorSchemaRoot toVectorSchemaRoot(Table table, BufferAllocator allocator) {
    Objects.requireNonNull(table, "table");
    Objects.requireNonNull(allocator, "allocator");
    List<Field> fields = new ArrayList<>(table.numColumns());
    for (Column column : table.columns()) {
      boolean nullable = column.nullable() || column.type() == DataType.NULL;
      fields.add(new Field(column.name(), new FieldType(nullable, arrowType(column), null), null));
    }
    Schema schema = new Schema(fields, table.metadata());
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    try {
      int rows = table.numRows();
      List<FieldVector> vectors = root.getFieldVectors();
      for (int i = 0; i < vectors.size(); i++) {
        FieldVector vector = vectors.get(i);
        vector.setInitialCapacity(rows);
        vector.allocateNew();
        fill(table.column(i), vector, rows);
      }
      root.setRowCount(rows);
      return root;
    } catch (RuntimeException | Error failed) {
      root.close();
      throw failed;
    }
  }

  private static ArrowType arrowType(Column column) {
    String zone = column.timezone().orElse(null);
    return switch (column.type()) {
      case NULL -> ArrowType.Null.INSTANCE;
      case BOOL -> ArrowType.Bool.INSTANCE;
      case INT8 -> new ArrowType.Int(8, true);
      case INT16 -> new ArrowType.Int(16, true);
      case INT32 -> new ArrowType.Int(32, true);
      case INT64 -> new ArrowType.Int(64, true);
      case UINT8 -> new ArrowType.Int(8, false);
      case UINT16 -> new ArrowType.Int(16, false);
      case UINT32 -> new ArrowType.Int(32, false);
      case UINT64 -> new ArrowType.Int(64, false);
      case FLOAT32 -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case FLOAT64 -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case UTF8 -> ArrowType.Utf8.INSTANCE;
      case LARGE_UTF8 -> ArrowType.LargeUtf8.INSTANCE;
      case BINARY -> ArrowType.Binary.INSTANCE;
      case LARGE_BINARY -> ArrowType.LargeBinary.INSTANCE;
      case DATE32 -> new ArrowType.Date(DateUnit.DAY);
      case DATE64 -> new ArrowType.Date(DateUnit.MILLISECOND);
      case TIME32_S -> new ArrowType.Time(TimeUnit.SECOND, 32);
      case TIME32_MS -> new ArrowType.Time(TimeUnit.MILLISECOND, 32);
      case TIME64_US -> new ArrowType.Time(TimeUnit.MICROSECOND, 64);
      case TIME64_NS -> new ArrowType.Time(TimeUnit.NANOSECOND, 64);
      case TIMESTAMP_S -> new ArrowType.Timestamp(TimeUnit.SECOND, zone);
      case TIMESTAMP_MS -> new ArrowType.Timestamp(TimeUnit.MILLISECOND, zone);
      case TIMESTAMP_US -> new ArrowType.Timestamp(TimeUnit.MICROSECOND, zone);
      case TIMESTAMP_NS -> new ArrowType.Timestamp(TimeUnit.NANOSECOND, zone);
    };
  }

  private static void fill(Column column, FieldVector vector, int rows) {
    DataType type = column.type();
    if (type == DataType.NULL) {
      return;
    }
    for (int row = 0; row < rows; row++) {
      if (column.isNull(row)) {
        continue;
      }
      if (vector instanceof BaseIntVector integers) {
        integers.setWithPossibleTruncate(row, column.getLong(row));
      } else if (vector instanceof BitVector bits) {
        bits.set(row, column.getBoolean(row) ? 1 : 0);
      } else if (vector instanceof Float4Vector floats) {
        floats.set(row, column.getFloat(row));
      } else if (vector instanceof Float8Vector doubles) {
        doubles.set(row, column.getDouble(row));
      } else if (vector instanceof BaseVariableWidthVector variable) {
        variable.setSafe(row, column.getBytes(row));
      } else if (vector instanceof BaseLargeVariableWidthVector large) {
        large.setSafe(row, column.getBytes(row));
      } else if (vector instanceof DateDayVector days) {
        days.set(row, (int) column.getLong(row));
      } else if (vector instanceof DateMilliVector millis) {
        millis.set(row, column.getLong(row));
      } else if (vector instanceof TimeSecVector seconds) {
        seconds.set(row, (int) column.getLong(row));
      } else if (vector instanceof TimeMilliVector millis) {
        millis.set(row, (int) column.getLong(row));
      } else if (vector instanceof TimeMicroVector micros) {
        micros.set(row, column.getLong(row));
      } else if (vector instanceof TimeNanoVector nanos) {
        nanos.set(row, column.getLong(row));
      } else if (vector instanceof TimeStampVector timestamps) {
        timestamps.set(row, column.getLong(row));
      } else {
        throw new IllegalStateException(
            "no Arrow conversion for column '" + column.name() + "' of type " + type);
      }
    }
  }
}
