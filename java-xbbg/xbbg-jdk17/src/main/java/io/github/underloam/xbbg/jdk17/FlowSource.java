package io.github.underloam.xbbg.jdk17;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * The two operations a {@link ViewPublisher} needs from a subscription view: an asynchronous read
 * that completes empty at the end of the stream, and a close.
 *
 * @param <T> the value type
 */
interface FlowSource<T> {
  /**
   * Starts the next read. The publisher never starts a read while another is in flight.
   *
   * @return the next value, or empty at the end of the stream
   */
  CompletableFuture<Optional<T>> next();

  /**
   * Closes the source and discards unread values.
   *
   * @return completion of the close
   */
  CompletableFuture<?> close();

  /**
   * Builds a source from two method references.
   *
   * @param next the read
   * @param close the close
   * @param <T> the value type
   * @return the source
   */
  static <T> FlowSource<T> of(
      Supplier<CompletableFuture<Optional<T>>> next,
      Supplier<? extends CompletableFuture<?>> close) {
    Objects.requireNonNull(next, "next");
    Objects.requireNonNull(close, "close");
    return new FlowSource<T>() {
      @Override
      public CompletableFuture<Optional<T>> next() {
        return next.get();
      }

      @Override
      public CompletableFuture<?> close() {
        return close.get();
      }
    };
  }
}
