package io.github.underloam.xbbg.subscription;

import java.util.HashMap;
import java.util.Map;

/**
 * One scalar row layout: field names, their kinds and a name lookup. Immutable: it adopts the
 * arrays it is given (copies from {@link io.github.underloam.xbbg.internal.TickBatch}), which
 * nobody modifies afterwards.
 */
final class TickLayout {
  final int version;
  final String[] fields;
  final byte[] kinds;
  private final Map<String, Integer> positions;

  TickLayout(int version, String[] fields, byte[] kinds) {
    this.version = version;
    this.fields = fields;
    this.kinds = kinds;
    Map<String, Integer> lookup = new HashMap<String, Integer>(fields.length * 2);
    for (int index = 0; index < fields.length; index++) {
      // Like a JavaScript Map built from entries: a repeated name resolves to its last index.
      lookup.put(fields[index], index);
    }
    this.positions = lookup;
  }

  /** Returns the field index of a name, or -1 when the layout has no such field. */
  int indexOf(String field) {
    Integer index = positions.get(field);
    return index == null ? -1 : index.intValue();
  }
}
