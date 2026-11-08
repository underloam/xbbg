package io.github.underloam.xbbg.internal;

import io.github.underloam.xbbg.Backend;

/**
 * Access to {@link Backend} internals from other packages (the subscription layer).
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class Backends {
  private static volatile Accessor accessor;

  private Backends() {}

  /** Implemented by {@link Backend}. */
  public interface Accessor {
    /**
     * Returns the native result kind a backend needs.
     *
     * @param backend the backend
     * @return {@code TABLE}, {@code IPC_STREAM} or {@code IPC_FILE}
     */
    ResultKind nativeKind(Backend<?> backend);

    /**
     * Converts a native result of {@link #nativeKind} to the backend's type.
     *
     * @param backend the backend
     * @param value the native result
     * @param <T> the result type
     * @return the converted result
     */
    <T> T fromNative(Backend<T> backend, Object value);
  }

  /**
   * Installs the accessor; called once by {@link Backend}'s class initializer.
   *
   * @param installed the accessor
   */
  public static void install(Accessor installed) {
    if (accessor != null) {
      throw new IllegalStateException("Backend accessor already installed");
    }
    accessor = installed;
  }

  /**
   * Returns the native result kind a backend needs.
   *
   * @param backend the backend
   * @return the kind
   */
  public static ResultKind nativeKind(Backend<?> backend) {
    return accessor().nativeKind(backend);
  }

  /**
   * Converts a native result to a backend's type.
   *
   * @param backend the backend
   * @param value the native result
   * @param <T> the result type
   * @return the converted result
   */
  public static <T> T fromNative(Backend<T> backend, Object value) {
    return accessor().fromNative(backend, value);
  }

  private static Accessor accessor() {
    Accessor result = accessor;
    if (result == null) {
      // Any Backend instance implies its class is initialized; this only runs on misuse.
      initialize(Backend.class);
      result = accessor;
    }
    return result;
  }

  static void initialize(Class<?> type) {
    try {
      Class.forName(type.getName(), true, type.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }
}
