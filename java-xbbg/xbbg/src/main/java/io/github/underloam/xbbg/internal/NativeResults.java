package io.github.underloam.xbbg.internal;

import io.github.underloam.xbbg.BlpInternalException;
import io.github.underloam.xbbg.table.Table;

/**
 * Checks that a native result has the Java type its result kind promises.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class NativeResults {
  private NativeResults() {}

  /**
   * Checks a native result against its kind. Kinds whose Java types belong to the subscription
   * layer ({@code TICKS}, {@code DELIVERY} and their lists) are passed through unchecked.
   *
   * @param kind the requested kind
   * @param value the native result
   * @return the value
   * @throws BlpInternalException if the value has the wrong type
   */
  public static Object check(ResultKind kind, Object value) {
    boolean ok;
    switch (kind) {
      case VOID:
        ok = value == null;
        break;
      case TABLE:
        ok = value instanceof Table;
        break;
      case IPC_STREAM:
      case IPC_FILE:
        ok = value instanceof byte[];
        break;
      case JSON:
        ok = value == null || value instanceof String;
        break;
      case HANDLE:
        ok = value instanceof Long;
        break;
      case BOOL:
        ok = value instanceof Boolean;
        break;
      case STRING:
        ok = value instanceof String;
        break;
      case TABLE_LIST:
        ok = value instanceof Table[];
        break;
      default:
        ok = true;
    }
    if (!ok) {
      throw new BlpInternalException(
          "native result for kind "
              + kind
              + " has the wrong type: "
              + (value == null ? "null" : value.getClass().getName()));
    }
    return value;
  }
}
