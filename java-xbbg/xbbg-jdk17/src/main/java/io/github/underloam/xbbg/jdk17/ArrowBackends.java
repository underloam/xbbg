package io.github.underloam.xbbg.jdk17;

import io.github.underloam.xbbg.Backend;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

/**
 * {@link Backend}s that return Arrow Java vectors. Requires {@code org.apache.arrow:arrow-vector}
 * and an allocator implementation ({@code arrow-memory-unsafe} or {@code arrow-memory-netty}) on
 * the class path, and {@code
 * --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED} on Java 17+ (see the
 * {@linkplain io.github.underloam.xbbg.jdk17 package documentation}).
 */
public final class ArrowBackends {
  private ArrowBackends() {}

  /**
   * A backend that reads results as one {@link VectorSchemaRoot}, allocated from {@code allocator}.
   * The engine sends an Arrow IPC stream ({@link Backend#IPC_STREAM}); its record batches are
   * concatenated into one root on the callback executor. The schema, including its metadata
   * (request ids, field errors, ...), is kept.
   *
   * <p>The caller owns each returned root and must {@link VectorSchemaRoot#close() close} it
   * (before closing the allocator).
   *
   * @param allocator the allocator for the result vectors
   * @return the backend
   */
  public static Backend<VectorSchemaRoot> vectorSchemaRoot(BufferAllocator allocator) {
    Objects.requireNonNull(allocator, "allocator");
    return Backend.map(Backend.IPC_STREAM, "vectorSchemaRoot", ipc -> read(ipc, allocator));
  }

  /**
   * Reads an Arrow IPC stream into one root.
   *
   * @param ipc the stream bytes
   * @param allocator the allocator for the result
   * @return a root owned by the caller
   * @throws UncheckedIOException if the stream is malformed
   */
  static VectorSchemaRoot read(byte[] ipc, BufferAllocator allocator) {
    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
      VectorSchemaRoot batch = reader.getVectorSchemaRoot();
      VectorSchemaRoot result = VectorSchemaRoot.create(batch.getSchema(), allocator);
      try {
        boolean first = true;
        while (reader.loadNextBatch()) {
          if (first) {
            // Take the first batch's buffers without copying.
            List<FieldVector> from = batch.getFieldVectors();
            List<FieldVector> to = result.getFieldVectors();
            for (int i = 0; i < from.size(); i++) {
              from.get(i).makeTransferPair(to.get(i)).transfer();
            }
            result.setRowCount(batch.getRowCount());
            first = false;
          } else {
            VectorSchemaRootAppender.append(result, batch);
          }
        }
        return result;
      } catch (IOException | RuntimeException failed) {
        result.close();
        throw failed;
      }
    } catch (IOException e) {
      throw new UncheckedIOException("failed to read the Arrow IPC stream", e);
    }
  }
}
