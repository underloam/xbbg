package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Backends;
import io.github.underloam.xbbg.internal.ResultKind;
import io.github.underloam.xbbg.table.RowsResult;
import io.github.underloam.xbbg.table.Table;
import java.util.Objects;
import java.util.function.Function;

/**
 * The Java form a tabular result is returned in. Methods that accept a backend also have an
 * overload without one, which returns a {@link Table}.
 *
 * @param <T> the result type
 */
public final class Backend<T> {
  /** An xbbg {@link Table} (the default). */
  public static final Backend<Table> TABLE =
      new Backend<Table>(
          "table",
          ResultKind.TABLE,
          new Function<Object, Table>() {
            @Override
            public Table apply(Object value) {
              return (Table) value;
            }
          });

  /** Row maps with the result metadata ({@link RowsResult}). */
  public static final Backend<RowsResult> ROWS =
      new Backend<RowsResult>(
          "rows",
          ResultKind.TABLE,
          new Function<Object, RowsResult>() {
            @Override
            public RowsResult apply(Object value) {
              return RowsResult.of((Table) value);
            }
          });

  /** Arrow IPC stream bytes. */
  public static final Backend<byte[]> IPC_STREAM =
      new Backend<byte[]>("ipcStream", ResultKind.IPC_STREAM, bytes());

  /** Arrow IPC file bytes (readable by Arrow tools that need the file format). */
  public static final Backend<byte[]> IPC_FILE =
      new Backend<byte[]>("ipcFile", ResultKind.IPC_FILE, bytes());

  static {
    Backends.install(
        new Backends.Accessor() {
          @Override
          public ResultKind nativeKind(Backend<?> backend) {
            return backend.kind;
          }

          @Override
          public <R> R fromNative(Backend<R> backend, Object value) {
            return backend.converter.apply(value);
          }
        });
  }

  private final String name;
  private final ResultKind kind;
  private final Function<Object, T> converter;

  private Backend(String name, ResultKind kind, Function<Object, T> converter) {
    this.name = name;
    this.kind = kind;
    this.converter = converter;
  }

  /**
   * Derives a backend that converts another backend's result, for example Arrow Java vectors from
   * {@link #IPC_STREAM} bytes.
   *
   * @param base the backend whose result is converted
   * @param name a name for messages
   * @param mapper the conversion, run on the callback executor
   * @param <R> the base result type
   * @param <T> the derived result type
   * @return the derived backend
   */
  public static <R, T> Backend<T> map(
      final Backend<R> base, String name, final Function<? super R, ? extends T> mapper) {
    Objects.requireNonNull(base, "base");
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(mapper, "mapper");
    return new Backend<T>(
        name,
        base.kind,
        new Function<Object, T>() {
          @Override
          public T apply(Object value) {
            return mapper.apply(base.converter.apply(value));
          }
        });
  }

  /**
   * Returns the backend's name.
   *
   * @return the name
   */
  public String name() {
    return name;
  }

  ResultKind nativeKind() {
    return kind;
  }

  T fromNative(Object value) {
    return converter.apply(value);
  }

  @Override
  public String toString() {
    return name;
  }

  private static Function<Object, byte[]> bytes() {
    return new Function<Object, byte[]>() {
      @Override
      public byte[] apply(Object value) {
        return (byte[]) value;
      }
    };
  }
}
