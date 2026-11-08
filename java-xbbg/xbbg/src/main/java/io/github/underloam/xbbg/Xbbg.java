package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Callbacks;
import io.github.underloam.xbbg.internal.Futures;
import io.github.underloam.xbbg.internal.NativeBridge;
import io.github.underloam.xbbg.internal.NativeLibraries;
import io.github.underloam.xbbg.internal.Versions;
import io.github.underloam.xbbg.sdk.BlpapiSource;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

/**
 * Library-wide entry points: versions, logging, Bloomberg runtime discovery, the configured engine
 * shared by the {@link Blp} functions, and helpers for futures.
 *
 * <p>Native libraries load on the first call that needs them. The Bloomberg runtime (blpapi) is
 * found from, in order: {@link #setSdkPath(Path)} or {@code -Dxbbg.blpapi.path}; {@code
 * BLPAPI_LIB_DIR}, {@code BLPAPI_ROOT}, {@code XBBG_DEV_SDK_ROOT}; a Bloomberg Terminal (DAPI)
 * install and, on Windows, {@code PATH}; a runtime provisioned into the xbbg cache; and finally a
 * download from {@link #setSdkSource(BlpapiSource)} or {@code -Dxbbg.sdk.source}/{@code
 * XBBG_SDK_SOURCE}.
 */
public final class Xbbg {
  private static final Object LOCK = new Object();
  private static EngineConfig configured;
  private static CompletableFuture<Engine> shared;

  private Xbbg() {}

  /**
   * Returns the version of this library.
   *
   * @return the version stamped at build time
   * @throws IllegalStateException if the build did not stamp the version
   */
  public static String version() {
    return Versions.version();
  }

  /**
   * Returns the version of the loaded native engine library. Loads the natives.
   *
   * @return the native crate version
   */
  public static String nativeVersion() {
    return NativeBridge.nativeVersion();
  }

  /**
   * Returns the version of the loaded Bloomberg runtime. Loads the natives.
   *
   * @return {@code major.minor.patch.build}
   */
  public static String sdkVersion() {
    return NativeBridge.sdkVersion();
  }

  /**
   * Sets the level of xbbg's native log (written to stderr; default {@code WARN}).
   *
   * @param level the level
   */
  public static void setLogLevel(LogLevel level) {
    NativeBridge.setLogLevel(Objects.requireNonNull(level, "level").wireValue());
  }

  /**
   * Returns the level of xbbg's native log.
   *
   * @return the level
   */
  public static LogLevel getLogLevel() {
    return LogLevel.parse(NativeBridge.getLogLevel());
  }

  /**
   * Sets where the Bloomberg runtime library is: the library file or a directory (a directory is
   * also searched as an SDK root). Takes precedence over every other source; a location without a
   * runtime is an error. Must be called before the natives load.
   *
   * @param path the library file or directory, or {@code null} to clear the setting
   * @throws IllegalStateException if the natives are already loaded
   */
  public static void setSdkPath(Path path) {
    NativeLibraries.setSdkPath(path);
  }

  /**
   * Sets where to download a Bloomberg runtime from when none is found locally (it is cached and
   * reused). Must be called before the natives load.
   *
   * @param source the source, or {@code null} to clear the setting
   * @throws IllegalStateException if the natives are already loaded
   */
  public static void setSdkSource(BlpapiSource source) {
    NativeLibraries.setSdkSource(source);
  }

  /**
   * Configures the shared engine used by {@link #engine()} and the {@link Blp} functions. The
   * previous shared engine, if any, is closed once its connection completes. The configuration is
   * validated when the engine connects.
   *
   * @param config the configuration
   * @return the stored configuration
   */
  public static EngineConfig configure(EngineConfig config) {
    Objects.requireNonNull(config, "config");
    replaceConfiguration(config);
    return config;
  }

  /**
   * Configures the shared engine to connect to a host and port.
   *
   * @param host the Terminal or server host
   * @param port the port
   * @return the stored configuration
   */
  public static EngineConfig configure(String host, int port) {
    return configure(EngineConfig.builder().host(host).port(port).build());
  }

  /** Restores the default shared-engine configuration ({@code localhost:8194}). */
  public static void resetConfiguration() {
    replaceConfiguration(null);
  }

  /**
   * Returns the shared engine, connecting it on first use. Concurrent callers share one connection
   * attempt; a failed attempt is forgotten, so the next call retries. Cancelling the returned
   * future does not affect the shared connection.
   *
   * @return the future engine
   */
  public static CompletableFuture<Engine> engine() {
    try {
      // Load (and possibly download) the natives before taking LOCK, so configure(), engine() and
      // resetConfiguration() on other threads never wait for a download.
      NativeLibraries.ensureLoaded();
    } catch (Throwable loadFailure) {
      return Futures.failed(loadFailure);
    }
    CompletableFuture<Engine> current;
    synchronized (LOCK) {
      current = shared;
      if (current == null) {
        final CompletableFuture<Engine> attempt =
            Engine.connect(configured == null ? EngineConfig.defaults() : configured);
        shared = attempt;
        current = attempt;
        Futures.onComplete(
            attempt,
            (engine, error) -> {
              if (error != null) {
                forgetFailedAttempt(attempt);
              }
            });
      }
    }
    return Futures.copy(current);
  }

  /** Forgets a failed shared connection attempt, unless a newer attempt has replaced it. */
  @SuppressWarnings("ReferenceEquality")
  private static void forgetFailedAttempt(CompletableFuture<Engine> attempt) {
    synchronized (LOCK) {
      // Suppressed: identity on purpose. Futures have no value equality (Error Prone's
      // UndefinedEquals); the question is whether the slot still holds this very attempt.
      if (shared == attempt) {
        shared = null;
      }
    }
  }

  /**
   * Connects a new, independent engine.
   *
   * @param config the configuration
   * @return the future engine
   */
  public static CompletableFuture<Engine> connect(EngineConfig config) {
    return Engine.connect(config);
  }

  /**
   * Connects a new, independent engine with the default configuration.
   *
   * @return the future engine
   */
  public static CompletableFuture<Engine> connect() {
    return Engine.connect();
  }

  /**
   * Waits for a future and returns its value, rethrowing its failure unwrapped (a {@link
   * BlpException}, {@link IllegalArgumentException}, ...). A cancelled future throws {@link
   * CancellationException}. If the waiting thread is interrupted, the interrupt flag is restored
   * and a {@link CancellationException} caused by the {@link InterruptedException} is thrown.
   *
   * @param future the future
   * @param <T> the value type
   * @return the value
   */
  public static <T> T await(CompletableFuture<T> future) {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      CancellationException interrupted =
          new CancellationException("interrupted while waiting for an xbbg result");
      interrupted.initCause(e);
      throw interrupted;
    } catch (ExecutionException | CompletionException e) {
      Throwable cause = Futures.unwrap(e.getCause());
      Futures.throwIfUnchecked(cause);
      throw new CompletionException(cause);
    }
  }

  /**
   * Returns the executor that completes every future of this library (default: a cached pool of
   * daemon threads named {@code xbbg-callback-N}).
   *
   * @return the executor
   */
  public static Executor callbackExecutor() {
    return Callbacks.executor();
  }

  /**
   * Replaces the executor that completes this library's futures. Dependent stages run on it unless
   * they use an async variant. If it rejects a completion, the default executor runs it.
   *
   * <p>Every native result is handed to this executor from a native bridge thread (the engine
   * library has two), inside the native call that delivers it. A same-thread executor (such as
   * {@code Runnable::run}) therefore runs the completion and every non-async dependent stage on a
   * bridge thread: a callback that blocks there, for example waiting for another xbbg result with
   * {@link #await} or a blocking subscription read, stalls every other native completion and can
   * deadlock. Use such an executor only for short, non-blocking callbacks, and size a bounded
   * executor so that callbacks waiting for xbbg results never occupy every thread.
   *
   * @param executor the executor, or {@code null} for the default
   */
  public static void setCallbackExecutor(Executor executor) {
    Callbacks.setExecutor(executor);
  }

  private static void replaceConfiguration(EngineConfig config) {
    CompletableFuture<Engine> old;
    synchronized (LOCK) {
      configured = config;
      old = shared;
      shared = null;
    }
    if (old != null) {
      Futures.onComplete(
          old,
          (engine, error) -> {
            if (engine != null) {
              engine.close();
            }
          });
    }
  }
}
