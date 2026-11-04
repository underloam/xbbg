package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Futures;
import io.github.underloam.xbbg.internal.JsonValue;
import io.github.underloam.xbbg.internal.NativeBridge;
import io.github.underloam.xbbg.internal.ResultKind;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A connection to Bloomberg: its own request and subscription sessions, field and schema caches and
 * worker threads. Engines are independent; {@link Xbbg#engine()} gives a shared, configured one.
 *
 * <p>Methods that touch Bloomberg return {@link CompletableFuture}s, completed on {@link
 * Xbbg#callbackExecutor()}; cancelling one cancels the native work behind it. {@link #close()}
 * shuts the engine down, which also ends its subscriptions. An engine that is garbage-collected
 * without being closed is only released: its subscriptions and calls in flight keep the native
 * engine running until they finish.
 */
public final class Engine extends EngineSubscriptions implements AutoCloseable {
  private Engine(long handle, KeyColumn keyColumn) {
    super(handle, keyColumn);
  }

  /**
   * Connects a new engine. Startup connects the Bloomberg sessions and opens the warm-up services,
   * which can take seconds.
   *
   * @param config the configuration
   * @return the future engine; cancelling it releases an engine that connects anyway
   */
  public static CompletableFuture<Engine> connect(EngineConfig config) {
    Objects.requireNonNull(config, "config");
    final CompletableFuture<Long> handle = NativeBridge.connect(config.toJson());
    final Futures.CancellableFuture<Engine> result =
        new Futures.CancellableFuture<Engine>(
            new Runnable() {
              @Override
              public void run() {
                handle.cancel(false);
              }
            });
    Futures.onComplete(
        handle,
        (value, error) -> {
          if (error != null) {
            result.completeExceptionally(Futures.unwrap(error));
            return;
          }
          Engine engine;
          try {
            engine = create(value);
          } catch (Throwable failure) {
            NativeBridge.engineClose(value);
            result.completeExceptionally(failure);
            return;
          }
          if (!result.complete(engine)) {
            engine.close();
          }
        });
    return result;
  }

  /**
   * Connects a new engine with the default configuration ({@code localhost:8194}).
   *
   * @return the future engine
   */
  public static CompletableFuture<Engine> connect() {
    return connect(EngineConfig.defaults());
  }

  /**
   * Connects a new engine and waits for it.
   *
   * @param config the configuration
   * @return the engine
   * @throws BlpException if the engine cannot start
   */
  public static Engine open(EngineConfig config) {
    return Xbbg.await(connect(config));
  }

  /**
   * Connects a new engine to a host and port and waits for it.
   *
   * @param host the Terminal or server host
   * @param port the port
   * @return the engine
   * @throws BlpException if the engine cannot start
   */
  public static Engine open(String host, int port) {
    return open(EngineConfig.builder().host(host).port(port).build());
  }

  /**
   * Connects a new engine with the default configuration and waits for it.
   *
   * @return the engine
   * @throws BlpException if the engine cannot start
   */
  public static Engine open() {
    return open(EngineConfig.defaults());
  }

  /**
   * Shuts the engine down and releases it: running subscriptions of this engine end (their reads
   * fail) and later calls fail with {@link IllegalStateException}. Idempotent.
   */
  @Override
  public void close() {
    shutdownAndRelease();
  }

  @Override
  public String toString() {
    return "Engine{keyColumn=" + keyColumn() + (isClosed() ? ", closed}" : "}");
  }

  /** Wraps a live engine handle (also used by the native lifecycle tests). */
  static Engine create(long handle) {
    String keyColumn =
        (String)
            NativeBridge.engineCallSync(
                handle, "keyColumn", JsonValue.EMPTY_OBJECT, ResultKind.STRING);
    return new Engine(handle, KeyColumn.parse(keyColumn));
  }
}
