package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Futures;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Base of the static {@link Blp} facade: every {@code Blp} function runs on the configured engine
 * ({@link Xbbg#engine()}), so connection errors come first.
 */
abstract class BlpBase {
  BlpBase() {}

  /**
   * Returns the configured engine, connecting it on first use.
   *
   * @return the future engine
   */
  protected static CompletableFuture<Engine> configuredEngine() {
    return Xbbg.engine();
  }

  /**
   * Runs an action on the configured engine. Cancelling the result cancels the action's future
   * (never the shared connection).
   *
   * @param action the call to make on the engine
   * @param <T> the result type
   * @return the future result
   */
  protected static <T> CompletableFuture<T> withEngine(
      Function<? super Engine, ? extends CompletionStage<T>> action) {
    return Futures.compose(Xbbg.engine(), action);
  }
}
