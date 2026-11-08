package io.github.underloam.xbbg.internal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads the schema (top-level field names and schema metadata) of Arrow IPC stream or file bytes
 * without decoding record batches: the leading schema message of a stream, or the footer of a file.
 * Every FlatBuffer read is bounds-checked.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class ArrowIpcSchema {
  private static final byte[] FILE_MAGIC = "ARROW1".getBytes(StandardCharsets.US_ASCII);
  private static final int HEADER_SCHEMA = 1;

  private final List<String> fieldNames;
  private final Map<String, String> metadata;

  private ArrowIpcSchema(List<String> fieldNames, Map<String, String> metadata) {
    this.fieldNames = Collections.unmodifiableList(fieldNames);
    this.metadata = Collections.unmodifiableMap(metadata);
  }

  /**
   * Reads the schema of Arrow IPC stream or file bytes.
   *
   * @param ipc the IPC bytes
   * @return the schema
   * @throws IllegalArgumentException if the bytes hold no valid schema
   */
  public static ArrowIpcSchema read(byte[] ipc) {
    ByteBuffer buffer = ByteBuffer.wrap(ipc).order(ByteOrder.LITTLE_ENDIAN);
    Flat flat;
    int schema;
    if (startsWithMagic(ipc) && endsWithMagic(ipc)) {
      // File format: the footer (Footer.schema, slot 1) is the authoritative schema; writers may
      // pad the leading magic beyond 8 bytes.
      int footerLength = readInt(buffer, ipc.length - 10);
      long footerStart = (long) ipc.length - 10 - footerLength;
      if (footerLength <= 0 || footerStart < 8) {
        throw malformed("invalid file footer length " + footerLength);
      }
      flat = new Flat(buffer, (int) footerStart, ipc.length - 10);
      schema = flat.table(flat.root(), 1);
    } else {
      int position = 0;
      int length = readInt(buffer, position);
      position += 4;
      if (length == -1) {
        length = readInt(buffer, position);
        position += 4;
      }
      if (length <= 0 || (long) position + length > ipc.length) {
        throw malformed("invalid schema message length " + length);
      }
      flat = new Flat(buffer, position, position + length);
      int message = flat.root();
      if (flat.ubyte(message, 1) != HEADER_SCHEMA) {
        throw malformed("the first IPC message is not a schema");
      }
      schema = flat.table(message, 2);
    }
    if (schema < 0) {
      throw malformed("the IPC data has no schema");
    }
    List<String> names = new ArrayList<String>();
    int fields = flat.vector(schema, 1);
    if (fields >= 0) {
      int count = flat.length(fields);
      for (int i = 0; i < count; i++) {
        int field = flat.tableAt(fields, i);
        String name = flat.string(field, 0);
        names.add(name == null ? "" : name);
      }
    }
    Map<String, String> metadata = new LinkedHashMap<String, String>();
    int entries = flat.vector(schema, 2);
    if (entries >= 0) {
      int count = flat.length(entries);
      for (int i = 0; i < count; i++) {
        int entry = flat.tableAt(entries, i);
        String key = flat.string(entry, 0);
        String value = flat.string(entry, 1);
        if (key != null) {
          metadata.put(key, value == null ? "" : value);
        }
      }
    }
    return new ArrowIpcSchema(names, metadata);
  }

  /**
   * Returns the top-level field names in schema order.
   *
   * @return an unmodifiable list
   */
  public List<String> fieldNames() {
    return fieldNames;
  }

  /**
   * Returns the schema metadata.
   *
   * @return an unmodifiable map
   */
  public Map<String, String> metadata() {
    return metadata;
  }

  private static boolean endsWithMagic(byte[] ipc) {
    if (ipc.length < 18) {
      return false;
    }
    int start = ipc.length - FILE_MAGIC.length;
    for (int i = 0; i < FILE_MAGIC.length; i++) {
      if (ipc[start + i] != FILE_MAGIC[i]) {
        return false;
      }
    }
    return true;
  }

  private static boolean startsWithMagic(byte[] ipc) {
    if (ipc.length < 8) {
      return false;
    }
    for (int i = 0; i < FILE_MAGIC.length; i++) {
      if (ipc[i] != FILE_MAGIC[i]) {
        return false;
      }
    }
    return true;
  }

  private static int readInt(ByteBuffer buffer, int position) {
    if (position < 0 || position + 4 > buffer.limit()) {
      throw malformed("truncated message prefix");
    }
    return buffer.getInt(position);
  }

  private static IllegalArgumentException malformed(String detail) {
    return new IllegalArgumentException("malformed Arrow IPC schema: " + detail);
  }

  /** Bounds-checked FlatBuffer reads within {@code [start, end)}. */
  private static final class Flat {
    private final ByteBuffer buffer;
    private final int start;
    private final int end;

    Flat(ByteBuffer buffer, int start, int end) {
      this.buffer = buffer;
      this.start = start;
      this.end = end;
    }

    int root() {
      return start + uint32(start);
    }

    /** Position of field {@code slot} of the table at {@code table}, or -1 when absent. */
    int field(int table, int slot) {
      int vtable = table - int32(table);
      int vtableSize = uint16(vtable);
      int offsetIndex = 4 + slot * 2;
      if (offsetIndex + 2 > vtableSize) {
        return -1;
      }
      int offset = uint16(vtable + offsetIndex);
      return offset == 0 ? -1 : table + offset;
    }

    int ubyte(int table, int slot) {
      int position = field(table, slot);
      if (position < 0) {
        return 0;
      }
      check(position, 1);
      return buffer.get(position) & 0xff;
    }

    int table(int table, int slot) {
      int position = field(table, slot);
      return position < 0 ? -1 : position + uint32(position);
    }

    int vector(int table, int slot) {
      return table(table, slot);
    }

    int length(int vector) {
      int length = uint32(vector);
      check(vector + 4, length * 4L);
      return length;
    }

    int tableAt(int vector, int index) {
      int element = vector + 4 + index * 4;
      return element + uint32(element);
    }

    String string(int table, int slot) {
      int position = table(table, slot);
      if (position < 0) {
        return null;
      }
      int length = uint32(position);
      check(position + 4, length);
      byte[] bytes = new byte[length];
      for (int i = 0; i < length; i++) {
        bytes[i] = buffer.get(position + 4 + i);
      }
      return new String(bytes, StandardCharsets.UTF_8);
    }

    private int uint16(int position) {
      check(position, 2);
      return buffer.getShort(position) & 0xffff;
    }

    private int int32(int position) {
      check(position, 4);
      return buffer.getInt(position);
    }

    private int uint32(int position) {
      int value = int32(position);
      if (value < 0) {
        throw malformed("offset out of range at " + position);
      }
      return value;
    }

    private void check(int position, long size) {
      if (position < start || size < 0 || position + size > end) {
        throw malformed("read of " + size + " bytes at " + position + " is out of bounds");
      }
    }
  }
}
