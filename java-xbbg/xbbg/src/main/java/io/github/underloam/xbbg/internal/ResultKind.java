package io.github.underloam.xbbg.internal;

/**
 * Result kinds of the native call protocol. The code is the {@code int} passed to and from the JNI
 * library.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public enum ResultKind {
  /** No value; the call completes with {@code null}. */
  VOID(0),
  /** A {@link io.github.underloam.xbbg.table.Table}. */
  TABLE(1),
  /** Arrow IPC stream bytes ({@code byte[]}). */
  IPC_STREAM(2),
  /** Arrow IPC file bytes ({@code byte[]}). */
  IPC_FILE(3),
  /** JSON text ({@code String}). */
  JSON(4),
  /** A native handle ({@code Long}). */
  HANDLE(5),
  /** A {@code Boolean}. */
  BOOL(6),
  /** A {@code String}. */
  STRING(7),
  /** A subscription tick batch. */
  TICKS(8),
  /** One subscription delivery. */
  DELIVERY(9),
  /** An array of tables ({@code Table[]}). */
  TABLE_LIST(10),
  /** A list of tick batches. */
  TICKS_LIST(11),
  /** A list of subscription deliveries. */
  DELIVERY_LIST(12);

  private static final ResultKind[] BY_CODE = values();

  private final int code;

  ResultKind(int code) {
    this.code = code;
  }

  /**
   * Returns the wire code.
   *
   * @return the {@code int} code used by the JNI library
   */
  public int code() {
    return code;
  }

  /**
   * Looks a kind up by its wire code.
   *
   * @param code the wire code
   * @return the kind
   * @throws IllegalArgumentException if no kind has this code
   */
  public static ResultKind fromCode(int code) {
    if (code >= 0 && code < BY_CODE.length && BY_CODE[code].code == code) {
      return BY_CODE[code];
    }
    throw new IllegalArgumentException("unknown result kind " + code);
  }

  /**
   * Whether a table-producing call can return this kind.
   *
   * @return {@code true} for {@link #TABLE}, {@link #IPC_STREAM} and {@link #IPC_FILE}
   */
  public boolean isTableKind() {
    return this == TABLE || this == IPC_STREAM || this == IPC_FILE;
  }
}
