package io.github.underloam.xbbg.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Future helpers whose derived futures propagate cancellation back to their source, so cancelling a
 * result future cancels the native call behind it.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class Futures {
  private Futures() {}

  /**
   * Returns a future that already failed.
   *
   * @param error the failure
   * @param <T> the value type
   * @return the failed future
   */
  public static <T> CompletableFuture<T> failed(Throwable error) {
    CompletableFuture<T> future = new CompletableFuture<T>();
    future.completeExceptionally(error);
    return future;
  }

  /**
   * Strips {@link CompletionException} and {@link ExecutionException} wrappers.
   *
   * @param error the error
   * @return the innermost wrapped error
   */
  public static Throwable unwrap(Throwable error) {
    Throwable current = error;
    while ((current instanceof CompletionException || current instanceof ExecutionException)
        && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  /**
   * Rethrows an unchecked failure unchanged: a {@link RuntimeException} or an {@link Error}.
   * Returns normally for anything else (a checked exception or {@code null}), which the caller
   * wraps.
   *
   * @param error the failure, already {@linkplain #unwrap(Throwable) unwrapped}
   */
  public static void throwIfUnchecked(Throwable error) {
    if (error instanceof RuntimeException) {
      throw (RuntimeException) error;
    }
    if (error instanceof Error) {
      throw (Error) error;
    }
  }

  /**
   * Runs an action when a future completes, for callers that route the outcome themselves: the
   * action completes the caller's own future (or only cleans up), so that future is where the
   * outcome is observed.
   *
   * @param future the future to watch
   * @param action the action, given the value, or {@code null} and the failure
   * @param <T> the value type
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public static <T> void onComplete(
      CompletableFuture<T> future, BiConsumer<? super T, ? super Throwable> action) {
    // Suppressed: nothing observes the stage whenComplete returns. It mirrors `future` (or the
    // action's own exception); every action reports its outcome through the future its caller
    // returns.
    future.whenComplete(action);
  }

  /**
   * Maps a future. The mapping runs on the thread that completes the source; an exception it throws
   * fails the result. Cancelling the result cancels the source.
   *
   * @param source the source future
   * @param mapper the mapping
   * @param <T> the source type
   * @param <R> the result type
   * @return the mapped future
   */
  public static <T, R> CompletableFuture<R> map(
      final CompletableFuture<T> source, final Function<? super T, ? extends R> mapper) {
    final CancellableFuture<R> result =
        new CancellableFuture<R>(
            new Runnable() {
              @Override
              public void run() {
                source.cancel(false);
              }
            });
    onComplete(
        source,
        (value, error) -> {
          if (error != null) {
            result.completeExceptionally(unwrap(error));
            return;
          }
          try {
            result.complete(mapper.apply(value));
          } catch (Throwable mappingError) {
            result.completeExceptionally(mappingError);
          }
        });
    return result;
  }

  /**
   * Chains a future into another stage. Cancelling the result cancels the source and, once it
   * exists, the inner stage.
   *
   * @param source the source future
   * @param next builds the next stage from the source value
   * @param <T> the source type
   * @param <R> the result type
   * @return the chained future
   */
  public static <T, R> CompletableFuture<R> compose(
      final CompletableFuture<T> source,
      final Function<? super T, ? extends CompletionStage<R>> next) {
    final AtomicReference<CompletableFuture<R>> inner = new AtomicReference<CompletableFuture<R>>();
    final CancellableFuture<R> result =
        new CancellableFuture<R>(
            new Runnable() {
              @Override
              public void run() {
                source.cancel(false);
                CompletableFuture<R> stage = inner.get();
                if (stage != null) {
                  stage.cancel(false);
                }
              }
            });
    onComplete(
        source,
        (value, error) -> {
          if (error != null) {
            result.completeExceptionally(unwrap(error));
            return;
          }
          CompletionStage<R> stage;
          try {
            stage = next.apply(value);
          } catch (Throwable chainError) {
            result.completeExceptionally(chainError);
            return;
          }
          if (stage == null) {
            result.completeExceptionally(new NullPointerException("next stage is null"));
            return;
          }
          CompletableFuture<R> innerFuture = stage.toCompletableFuture();
          inner.set(innerFuture);
          if (result.isCancelled()) {
            innerFuture.cancel(false);
            return;
          }
          onComplete(
              innerFuture,
              (innerValue, innerError) -> {
                if (innerError != null) {
                  result.completeExceptionally(unwrap(innerError));
                } else {
                  result.complete(innerValue);
                }
              });
        });
    return result;
  }

  /**
   * Chains the next stage once a future settles either way: {@code handle(next)} followed by {@code
   * thenCompose(stage -> stage)}, without the nested future in between. The result fails when
   * {@code next} throws, returns {@code null} or returns a stage that fails.
   *
   * @param source the source future
   * @param next builds the next stage from the source value, or {@code null} and the failure
   * @param <T> the source type
   * @param <R> the result type
   * @return the chained future
   */
  public static <T, R> CompletableFuture<R> handleCompose(
      CompletableFuture<T> source,
      BiFunction<? super T, Throwable, ? extends CompletionStage<R>> next) {
    final CompletableFuture<R> result = new CompletableFuture<R>();
    onComplete(
        source,
        (value, error) -> {
          CompletionStage<R> stage;
          try {
            stage = next.apply(value, error);
          } catch (Throwable chainError) {
            result.completeExceptionally(chainError);
            return;
          }
          if (stage == null) {
            result.completeExceptionally(new NullPointerException("next stage is null"));
            return;
          }
          onComplete(
              stage.toCompletableFuture(),
              (nextValue, nextError) -> {
                if (nextError != null) {
                  result.completeExceptionally(nextError);
                } else {
                  result.complete(nextValue);
                }
              });
        });
    return result;
  }

  /**
   * Returns a view of a shared future: it completes with the source, but cancelling it leaves the
   * source untouched.
   *
   * @param shared the shared future
   * @param <T> the value type
   * @return an independent copy
   */
  public static <T> CompletableFuture<T> copy(CompletableFuture<T> shared) {
    final CompletableFuture<T> copy = new CompletableFuture<T>();
    onComplete(
        shared,
        (value, error) -> {
          if (error != null) {
            copy.completeExceptionally(unwrap(error));
          } else {
            copy.complete(value);
          }
        });
    return copy;
  }

  /**
   * A future that runs an action once when it is cancelled.
   *
   * @param <T> the value type
   */
  public static class CancellableFuture<T> extends CompletableFuture<T> {
    private final Runnable onCancel;
    private final AtomicBoolean cancelRan = new AtomicBoolean();

    /**
     * Creates the future.
     *
     * @param onCancel the action run after a successful {@link #cancel(boolean)}
     */
    public CancellableFuture(Runnable onCancel) {
      this.onCancel = onCancel;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      boolean cancelled = super.cancel(mayInterruptIfRunning);
      if (cancelled && cancelRan.compareAndSet(false, true)) {
        onCancel.run();
      }
      return cancelled;
    }
  }
}
