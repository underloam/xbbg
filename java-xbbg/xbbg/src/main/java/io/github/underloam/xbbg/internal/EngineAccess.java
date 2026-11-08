package io.github.underloam.xbbg.internal;

import io.github.underloam.xbbg.Backend;
import io.github.underloam.xbbg.Engine;
import java.util.concurrent.CompletableFuture;

/**
 * Access to {@link Engine} call helpers from packages other than the root package (the {@code ext}
 * and {@code subscription} layers).
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class EngineAccess {
  private static volatile Accessor accessor;

  private EngineAccess() {}

  /** Implemented by the engine base class. */
  public interface Accessor {
    /**
     * See {@link EngineAccess#callTable}.
     *
     * @param engine the engine
     * @param method the method name
     * @param args the argument object
     * @param backend the result backend
     * @param warnDelayed whether to emit delayed-data warnings
     * @param <T> the result type
     * @return the future result
     */
    <T> CompletableFuture<T> callTable(
        Engine engine, String method, JsonValue args, Backend<T> backend, boolean warnDelayed);

    /**
     * See {@link EngineAccess#call}.
     *
     * @param engine the engine
     * @param method the method name
     * @param args the argument object
     * @param kind the result kind
     * @return the future result
     */
    CompletableFuture<Object> call(Engine engine, String method, JsonValue args, ResultKind kind);

    /**
     * See {@link EngineAccess#callSync}.
     *
     * @param engine the engine
     * @param method the method name
     * @param args the argument object
     * @param kind the result kind
     * @return the result
     */
    Object callSync(Engine engine, String method, JsonValue args, ResultKind kind);

    /**
     * See {@link EngineAccess#handle}.
     *
     * @param engine the engine
     * @return the native handle
     */
    long handle(Engine engine);
  }

  /**
   * Installs the accessor; called once by the engine base class initializer.
   *
   * @param installed the accessor
   */
  public static void install(Accessor installed) {
    if (accessor != null) {
      throw new IllegalStateException("Engine accessor already installed");
    }
    accessor = installed;
  }

  /**
   * Calls a table-producing engine method and converts the result to the backend, after the same
   * warnings {@code Engine.request} emits: delayed data (when {@code warnDelayed}) and the
   * once-per-JVM {@code ticker} key-column deprecation.
   *
   * @param engine the engine
   * @param method the method name
   * @param args the argument object
   * @param backend the result backend
   * @param warnDelayed whether to emit delayed-data warnings ({@code onDelayed = WARN})
   * @param <T> the result type
   * @return the future result; cancelling it cancels the native call
   */
  public static <T> CompletableFuture<T> callTable(
      Engine engine, String method, JsonValue args, Backend<T> backend, boolean warnDelayed) {
    return accessor().callTable(engine, method, args, backend, warnDelayed);
  }

  /**
   * Calls an async engine method.
   *
   * @param engine the engine
   * @param method the method name
   * @param args the argument object
   * @param kind the result kind
   * @return the future result; cancelling it cancels the native call
   */
  public static CompletableFuture<Object> call(
      Engine engine, String method, JsonValue args, ResultKind kind) {
    return accessor().call(engine, method, args, kind);
  }

  /**
   * Calls a sync engine method on the calling thread.
   *
   * @param engine the engine
   * @param method the method name
   * @param args the argument object
   * @param kind the result kind
   * @return the result
   */
  public static Object callSync(Engine engine, String method, JsonValue args, ResultKind kind) {
    return accessor().callSync(engine, method, args, kind);
  }

  /**
   * Returns an engine's native handle.
   *
   * @param engine the engine
   * @return the handle
   * @throws IllegalStateException if the engine is closed
   */
  public static long handle(Engine engine) {
    return accessor().handle(engine);
  }

  private static Accessor accessor() {
    Accessor result = accessor;
    if (result == null) {
      Backends.initialize(Engine.class);
      result = accessor;
    }
    return result;
  }
}
