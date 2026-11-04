package io.github.underloam.xbbg.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/** Fixtures were written by pyarrow 25 (stream and file format of one 3-column batch). */
class ArrowIpcSchemaTest {
  private static byte[] fixture(String name) throws IOException {
    try (InputStream in = ArrowIpcSchemaTest.class.getResourceAsStream(name)) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buffer = new byte[4096];
      int read;
      while ((read = in.read(buffer)) > 0) {
        out.write(buffer, 0, read);
      }
      return out.toByteArray();
    }
  }

  @Test
  void readsTheStreamFormatSchema() throws IOException {
    ArrowIpcSchema schema = ArrowIpcSchema.read(fixture("sample-stream.arrows"));
    assertEquals(Arrays.asList("ticker", "PX_LAST", "date"), schema.fieldNames());
    assertEquals("caf\u00e9", schema.metadata().get("note"));
    assertTrue(schema.metadata().get("xbbg.delayed").contains("\"delay_minutes\":15"));
    assertEquals(2, schema.metadata().size());
  }

  @Test
  void readsTheFileFormatSchema() throws IOException {
    ArrowIpcSchema schema = ArrowIpcSchema.read(fixture("sample-file.arrow"));
    assertEquals(Arrays.asList("ticker", "PX_LAST", "date"), schema.fieldNames());
    assertEquals("caf\u00e9", schema.metadata().get("note"));
  }

  @Test
  void rejectsTruncatedOrForeignBytes() throws IOException {
    final byte[] stream = fixture("sample-stream.arrows");
    assertThrows(
        IllegalArgumentException.class, () -> ArrowIpcSchema.read(Arrays.copyOf(stream, 40)));
    assertThrows(IllegalArgumentException.class, () -> ArrowIpcSchema.read(new byte[] {1, 2}));
    assertThrows(IllegalArgumentException.class, () -> ArrowIpcSchema.read(new byte[0]));
    byte[] corrupt = stream.clone();
    for (int i = 8; i < 40; i++) {
      corrupt[i] = (byte) 0xff;
    }
    assertThrows(IllegalArgumentException.class, () -> ArrowIpcSchema.read(corrupt));
  }
}
